import { Client as PgClient, type ClientConfig as PgClientConfig } from "pg";
import type { JobQueue, QueueConfig } from "../Queues";
import { randomUUID } from "node:crypto";
import { Worker } from "node:worker_threads";
import { type LabelledMessageEvent, requireLabel } from "../messages";

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

const parseConfig = (configData: string): ValidConfig | null => {
	let parsedJson: object | null;
	try {
		parsedJson = JSON.parse(configData);
	} catch (e) {
		return null;
	}
	if (
		!(
			parsedJson !== null &&
			"queues" in parsedJson &&
			"executorPaths" in parsedJson &&
			"postgresConn" in parsedJson
		)
	) {
		// Config is invalid
		return null;
	}

	return parsedJson as ValidConfig;
};

const startWorker = (config: ValidConfig): Worker | null => {
	const worker = new Worker(new URL("./job-worker.js", import.meta.url));

	return worker;
};

const init = async (configData: string) => {
	const config = parseConfig(configData);

	if (config === null) {
		throw new Error("Invalid Worker setup data");
	}

	const postgresClient = new PgClient(config.postgresConn);
	await postgresClient.connect();

	const queues = new Map<string, JobQueue>();
	for (const [name, queueConfig] of Object.entries(config.queues)) {
		queues.set(name, {
			name,
			activeJobs: [],
			maxConcurrency: queueConfig.maxConcurrency,
		});
	}

	const workerPool: WorkerPool = [];
	for (let i = 0; i < DEFAULT_WORKER_POOL_SIZE; i++) {
		const id = randomUUID();
		const worker = startWorker(config);
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
		postgresClient,
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
type Job = {
	id: string;
	worker: string;
	queue: string;
	args: unknown;
	insertedAt: Date;
	completedAt: Date | null;
	state: "available" | "completed" | "cancelled" | "UNKNOWN";
	attempts: number;
	maxAttempts: number;
};
const jobFromSchema = (dbJob: _JobSchema): Job => {
	return {
		id: String(dbJob.id),
		worker: dbJob.worker,
		queue: dbJob.queue,
		args: JSON.parse(dbJob.args),
		insertedAt: dbJob.inserted_at,
		completedAt: dbJob.completed_at,
		state:
			dbJob.state === "available" ||
			dbJob.state === "completed" ||
			dbJob.state === "cancelled"
				? dbJob.state
				: "UNKNOWN",
		attempts: dbJob.attempts,
		maxAttempts: dbJob.max_attempts,
	};
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

  const markJobExecuting = async (
    jobId: Job["id"]
  ) => postgresClient.query(
    `UPDATE jobs SET state = 'executing', attempts = attempts +1 WHERE id = $1::int4;`, [jobId]
  );

	return {
		getFirstAvailableJobForQueue,
		markJobExecuting
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
	postgresClient: PgClient,
) => {
	let lastUsedWorkerIndex = -1;
	const repo = createRepo(postgresClient);

	while (true) {
		const readyWorkers = filterReadyWorkers(workerPool);
		if (readyWorkers.length === 0) {
			await new Promise((resolve) => {
				setTimeout(resolve, 3000);
			});
			continue;
		}

		let shouldSleep = true;

		for (const [queueName, queue] of queues.entries()) {
			const jobsForQueue = await repo.getFirstAvailableJobForQueue(queueName);

			if (!jobsForQueue || jobsForQueue.rowCount === 0) {
				continue;
			}

			const job = jobsForQueue.rows[0];

			if (queue.activeJobs.length >= queue.maxConcurrency) {
			  console.log("reached concurrency limits")
        continue;
			}

      lastUsedWorkerIndex = await assignJobToWorker(queue, job, repo, readyWorkers, lastUsedWorkerIndex);
      shouldSleep = false;
		}

		if (shouldSleep) {
      await new Promise(resolve => {
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
    markJobExecuting: (id: Job["id"]) => Promise<unknown>
  },
  workerPool: WorkerPool,
  lastUsedWorkerIndex: number
): Promise<number> => {
  const [worker, newIndex] = roundRobinSelectWorker(
    workerPool,
    lastUsedWorkerIndex
  );

  addActiveJobToQueue(queue, job.id);

  await repo.markJobExecuting(job.id);

  worker.postMessage({
    label: "execute",
    job
  });
  // From here, the Worker updates execution state.

  return newIndex;
}

const handleMessage = async (
	event: LabelledMessageEvent<{ config: string }>,
) => {
	switch (event.data.label) {
		case "init": {
			const { queues, workerPool, postgresClient } = await init(
				event.data.config,
			);
			mainLoop(queues, workerPool, postgresClient);
			break;
		}
		default:
			console.log(`Unknown event kind: ${JSON.stringify(event)}`);
	}
};
self.onmessage = requireLabel(handleMessage);
