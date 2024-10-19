import { Client as PgClient } from "pg";
import { randomUUID } from "node:crypto";
import { Worker, parentPort } from "node:worker_threads";
import { requireLabel } from "../messages";
const DEBUG = false;
const Logger = {
	debug: (...message) => {
		if (DEBUG) {
			console.debug("[CONTROLLER]", ...message);
		}
	},
	warn: (...message) => {
		console.warn("[CONTROLLER]", ...message);
	},
	error: (...message) => {
		console.error("[CONTROLLER]", ...message);
	},
	log: (...message) => {
		console.log("[CONTROLLER]", ...message);
	},
};
const DEFAULT_WORKER_POOL_SIZE = 4;
const parseConfig = (configData) => {
	if (
		!(
			configData !== null &&
			"queues" in configData &&
			"executorPaths" in configData &&
			"postgresConn" in configData
		)
	) {
		// Config is invalid
		return null;
	}
	return configData;
};
const startWorker = (config, id, workerPool, queues, repo) => {
	const worker = new Worker(new URL("./job-worker.js", import.meta.url));
	worker.on("message", async (event) => {
		switch (event.label) {
			case "worker_state_change": {
				const poolWorker = workerPool.find((worker) => worker.id === event.id);
				if (!poolWorker) return;
				poolWorker.state = event.state;
				Logger.debug(`Updating state of worker ${event.id} to ${event.state}`);
				break;
			}
			case "job_state_change": {
				Logger.debug(`Job state changed: ${JSON.stringify(event)}`);
				// Remove active job from queue
				const queue = queues.get(event.queue);
				const { status, id: jobId } = event;
				if (!queue) {
					Logger.warn(
						`Received state change for unknown queue: ${JSON.stringify(event)}`,
					);
					return;
				}
				queue.activeJobs = queue.activeJobs.filter(
					(job) => job.id !== event.id,
				);
				if (status === "completed") {
					await repo.updateJobStatus(jobId, "completed");
				} else {
					await repo.markJobRetryable(jobId);
				}
			}
		}
	});
	worker.postMessage({
		label: "init",
		config: {
			executorPaths: config.executorPaths,
			postgresConfig: config.postgresConn,
			id,
		},
	});
	return worker;
};
const init = async (configData) => {
	const config = parseConfig(configData);
	if (config === null) {
		throw new Error("Invalid Controller setup data");
	}
	const postgresClient = new PgClient(config.postgresConn);
	await postgresClient.connect();
	const repo = createRepo(postgresClient);
	const queues = new Map();
	Logger.debug("Creating queues from", config.queues);
	for (const queueConfig of config.queues) {
		queues.set(queueConfig.name, {
			name: queueConfig.name,
			activeJobs: [],
			maxConcurrency: queueConfig.maxConcurrency,
		});
	}
	const workerPool = [];
	for (let i = 0; i < DEFAULT_WORKER_POOL_SIZE; i++) {
		const id = randomUUID();
		const worker = startWorker(config, id, workerPool, queues, repo);
		if (!worker) {
			// TODO: Log why worker was not created
			continue;
		}
		workerPool.push({
			id,
			state: "starting",
			worker,
		});
	}
	return {
		queues,
		workerPool,
		repo,
	};
};
const jobFromSchema = (dbJob) => {
	return {
		id: String(dbJob.id),
		worker: dbJob.worker,
		queue: dbJob.queue,
		args: dbJob.args,
		insertedAt: dbJob.inserted_at,
		completedAt: dbJob.completed_at,
		scheduledAt: dbJob.scheduled_at,
		state:
			dbJob.state === "available" ||
			dbJob.state === "completed" ||
			dbJob.state === "cancelled" ||
			dbJob.state === "executing" ||
			dbJob.state === "retryable"
				? dbJob.state
				: "UNKNOWN",
		attempts: dbJob.attempts,
		maxAttempts: dbJob.max_attempts,
	};
};
const FINAL_STATES = new Set(["completed", "cancelled", "discarded"]);
const isFinalState = (status) => {
	return FINAL_STATES.has(status);
};
const createRepo = (postgresClient) => {
	const getFirstAvailableJobForQueue = async (queueName) => {
		const result = await postgresClient.query(
			`SELECT
					*
				FROM
					jobs
				WHERE
					(state = 'available'
						OR state = 'retryable')
					AND queue = $1::text
					AND scheduled_at <= NOW()
				LIMIT 1;`,
			[queueName],
		);
		if (!result) return null;
		return {
			rowCount: result.rowCount ?? 0,
			rows: result.rows.map(jobFromSchema),
		};
	};
	const markJobExecuting = async (jobId) =>
		postgresClient.query(
			`UPDATE jobs SET state = 'executing', attempts = attempts +1 WHERE id = $1::int4;`,
			[jobId],
		);
	return {
		getFirstAvailableJobForQueue,
		markJobExecuting,
		async updateJobStatus(jobId, state) {
			return postgresClient.query(
				`UPDATE jobs SET state = $1::text ${isFinalState(state) ? ", completed_at = NOW()" : ""} WHERE id = $2::int4;`,
				[state, jobId],
			);
		},
		async markJobRetryable(jobId) {
			const job = (
				await postgresClient.query(`SELECT * FROM jobs WHERE id = $1::int4;`, [
					jobId,
				])
			).rows[0];
			if (job.attempts >= job.max_attempts) {
				return postgresClient.query(
					`UPDATE jobs SET state = $1::text, completed_at = NOW() WHERE id = $2::int4;`,
					["cancelled", jobId],
				);
			}
			const exponentialDelayMs = Math.pow(2, job.attempts) * 1000;
			const futureDate = new Date(Date.now() + exponentialDelayMs);
			return postgresClient.query(
				`UPDATE jobs SET
					state = 'retryable',
					scheduled_at = $1::timestamp
				 WHERE id = $2::int4;`,
				[futureDate.toISOString(), jobId],
			);
		},
	};
};
const filterReadyWorkers = (p) => {
	return p.filter((worker) => {
		return worker.state === "ready";
	});
};
const mainLoop = async (queues, workerPool, repo) => {
	let lastUsedWorkerIndex = -1;
	while (true) {
		const readyWorkers = filterReadyWorkers(workerPool);
		if (readyWorkers.length === 0) {
			Logger.debug("No workers ready; sleeping for 3s");
			await new Promise((resolve) => {
				setTimeout(resolve, 3000);
			});
			continue;
		}
		let shouldSleep = true;
		for (const [queueName, queue] of queues.entries()) {
			Logger.debug(
				`Checking for jobs in queue ${queueName} ${JSON.stringify(queue)}`,
			);
			const jobsForQueue = await repo.getFirstAvailableJobForQueue(queueName);
			if (!jobsForQueue || jobsForQueue.rowCount === 0) {
				continue;
			}
			const job = jobsForQueue.rows[0];
			if (queue.activeJobs.length >= queue.maxConcurrency) {
				Logger.debug(`Reached concurrency limits for ${queue.name} queue`);
				continue;
			}
			lastUsedWorkerIndex = await assignJobToWorker(
				queue,
				job,
				repo,
				readyWorkers,
				lastUsedWorkerIndex,
			);
			shouldSleep = false;
		}
		if (shouldSleep) {
			await new Promise((resolve) => {
				setTimeout(resolve, 1000);
			});
		}
	}
};
const roundRobinSelectWorker = (p, lastUsedIndex) => {
	const nextIndex = (lastUsedIndex + 1) % p.length;
	const worker = p[nextIndex].worker;
	return [worker, nextIndex];
};
const addActiveJobToQueue = (queue, id) => {
	queue.activeJobs.push({
		id,
		startedAt: new Date(),
	});
};
const assignJobToWorker = async (
	queue,
	job,
	repo,
	workerPool,
	lastUsedWorkerIndex,
) => {
	const [worker, newIndex] = roundRobinSelectWorker(
		workerPool,
		lastUsedWorkerIndex,
	);
	addActiveJobToQueue(queue, job.id);
	await repo.markJobExecuting(job.id);
	worker.postMessage({
		label: "execute",
		job,
	});
	// From here, the Worker updates execution state.
	return newIndex;
};
const handleMessage = async (event) => {
	switch (event.label) {
		case "init": {
			const { queues, workerPool, repo } = await init(event.config);
			mainLoop(queues, workerPool, repo);
			break;
		}
		default: {
		}
	}
};
if (!parentPort) throw Error();
parentPort.on("message", requireLabel(handleMessage));
