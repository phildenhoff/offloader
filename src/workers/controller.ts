import { Client as PgClient, type ClientConfig as PgClientConfig } from "pg";
import type { JobQueue, QueueConfig } from "../Queues";
import { randomUUID } from "node:crypto";
import { Worker, parentPort } from "node:worker_threads";
import { type LabelledMessageEvent, requireLabel } from "../messages";

const DEBUG = false;

const Logger = {
	debug: (...message: any) => {
		if (DEBUG) {
			console.debug("[CONTROLLER]", ...message);
		}
	},
	warn: (...message: any) => {
		console.warn("[CONTROLLER]", ...message);
	},
	error: (...message: any) => {
		console.error("[CONTROLLER]", ...message);
	},
	log: (...message: any) => {
		console.log("[CONTROLLER]", ...message);
	},
};

const DEFAULT_WORKER_POOL_SIZE = 4;

type ValidConfig = {
	postgresConn: PgClientConfig;
	queues: QueueConfig[];
	executorPaths: Record<string, string>;
};
type WorkerPool = {
	id: string;
	state: "starting" | "ready" | "off";
	worker: Worker;
}[];

const parseConfig = (configData: object): ValidConfig | null => {
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

	return configData as ValidConfig;
};

const startWorker = (
	config: ValidConfig,
	id: string,
	workerPool: WorkerPool,
	queues: Map<string, JobQueue>,
	repo: ReturnType<typeof createRepo>,
): Worker | null => {
	const worker = new Worker(new URL("./job-worker.js", import.meta.url));

	worker.on("message", async (event) => {
		switch (event.label) {
			case "worker_state_change": {
				const poolWorker = workerPool.find((worker) => worker.id === event.id);
				if (!poolWorker) return;

				poolWorker.state = event.state;
				console.log(
					"[CONTROLLER]",
					`Updating state of worker ${event.id} to ${event.state}`,
				);
				break;
			}
			case "job_state_change": {
				Logger.log(`Job state changed: ${JSON.stringify(event)}`);

				// Remove active job from queue
				const queue = queues.get(event.queue);
				const { status, id: jobId } = event;
				console.log(
					`New status for job id ${jobId} is ${status}. = completed? ${
						status === "completed"
					}`,
				);
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

const init = async (configData: object) => {
	const config = parseConfig(configData);

	if (config === null) {
		throw new Error("Invalid Controller setup data");
	}

	const postgresClient = new PgClient(config.postgresConn);
	await postgresClient.connect();
	const repo = createRepo(postgresClient);

	const queues = new Map<string, JobQueue>();
	Logger.debug("Creating queues from", config.queues);
	for (const queueConfig of config.queues) {
		queues.set(queueConfig.name, {
			name: queueConfig.name,
			activeJobs: [],
			maxConcurrency: queueConfig.maxConcurrency,
		});
	}

	const workerPool: WorkerPool = [];
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

type _JobSchema = {
	id: number;
	worker: string;
	queue: string;
	args: string;
	inserted_at: Date;
	completed_at: Date | null;
	state: string;

	// "smallint"/"int2"
	attempts: number;
	// "smallint"/"int2"
	max_attempts: number;
};
export type Job = {
	id: string;
	worker: string;
	queue: string;
	args: unknown;
	insertedAt: Date;
	completedAt: Date | null;
	state:
		| "available"
		| "completed"
		| "cancelled"
		| "executing"
		| "retryable"
		| "UNKNOWN";
	attempts: number;
	maxAttempts: number;
};
const jobFromSchema = (dbJob: _JobSchema): Job => {
	return {
		id: String(dbJob.id),
		worker: dbJob.worker,
		queue: dbJob.queue,
		args: dbJob.args,
		insertedAt: dbJob.inserted_at,
		completedAt: dbJob.completed_at,
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
const isFinalState = (status: string) => {
	return FINAL_STATES.has(status);
};
const createRepo = (postgresClient: PgClient) => {
	const getFirstAvailableJobForQueue = async (
		queueName: string,
	): Promise<{
		rowCount: number;
		rows: Job[];
	} | null> => {
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

	const markJobExecuting = async (jobId: Job["id"]) =>
		postgresClient.query(
			`UPDATE jobs SET state = 'executing', attempts = attempts +1 WHERE id = $1::int4;`,
			[jobId],
		);

	return {
		getFirstAvailableJobForQueue,
		markJobExecuting,
		async updateJobStatus(jobId: Job["id"], state: string) {
			return postgresClient.query(
				`UPDATE jobs SET state = $1::text ${isFinalState(state) ? ", completed_at = NOW()" : ""} WHERE id = $2::int4;`,
				[state, jobId],
			);
		},
		async markJobRetryable(jobId: Job["id"]) {
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

const filterReadyWorkers = (p: WorkerPool) => {
	return p.filter((worker) => {
		return worker.state === "ready";
	});
};

const mainLoop = async (
	queues: Map<string, JobQueue>,
	workerPool: WorkerPool,
	repo: ReturnType<typeof createRepo>,
) => {
	let lastUsedWorkerIndex = -1;
	Logger.log("Starting main loop");

	while (true) {
		const readyWorkers = filterReadyWorkers(workerPool);
		if (readyWorkers.length === 0) {
			Logger.log("No workers ready; sleeping for 3s");
			await new Promise((resolve) => {
				setTimeout(resolve, 3000);
			});
			continue;
		}

		let shouldSleep = true;

		for (const [queueName, queue] of queues.entries()) {
			Logger.log(
				`Checking for jobs in queue ${queueName} ${JSON.stringify(queue)}`,
			);
			const jobsForQueue = await repo.getFirstAvailableJobForQueue(queueName);

			if (!jobsForQueue || jobsForQueue.rowCount === 0) {
				continue;
			}

			const job = jobsForQueue.rows[0];

			if (queue.activeJobs.length >= queue.maxConcurrency) {
				Logger.log(`Reached concurrency limits for ${queue.name} queue`);
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

const roundRobinSelectWorker = (
	p: WorkerPool,
	lastUsedIndex: number,
): [Worker, number] => {
	const nextIndex = (lastUsedIndex + 1) % p.length;
	const worker = p[nextIndex].worker;

	return [worker, nextIndex];
};

const addActiveJobToQueue = (queue: JobQueue, id: Job["id"]) => {
	queue.activeJobs.push({
		id,
		startedAt: new Date(),
	});
};

const assignJobToWorker = async (
	queue: JobQueue,
	job: Job,
	repo: {
		markJobExecuting: (id: Job["id"]) => Promise<unknown>;
	},
	workerPool: WorkerPool,
	lastUsedWorkerIndex: number,
): Promise<number> => {
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

const handleMessage = async (
	event: LabelledMessageEvent<{ config: object }>,
) => {
	switch (event.label) {
		case "init": {
			const { queues, workerPool, repo } = await init(event.config);
			mainLoop(queues, workerPool, repo);
			break;
		}
		default:
			console.log(`Unknown event kind: ${JSON.stringify(event)}`);
	}
};
if (!parentPort) throw Error();
parentPort.on("message", requireLabel(handleMessage));
