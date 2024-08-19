import { Client as PgClient } from "pg";
import { randomUUID } from "node:crypto";
import { Worker, parentPort } from "node:worker_threads";
import { requireLabel } from "../messages";
const DEFAULT_WORKER_POOL_SIZE = 4;
const parseConfig = (configData) => {
    if (!(configData !== null &&
        "queues" in configData &&
        "executorPaths" in configData &&
        "postgresConn" in configData)) {
        // Config is invalid
        return null;
    }
    return configData;
};
const startWorker = (config, id, workerPool) => {
    const worker = new Worker(new URL("./job-worker.js", import.meta.url));
    worker.on('message', (event) => {
        switch (event.label) {
            case "worker_state_change": {
                const poolWorker = workerPool.find((worker) => worker.id === event.id);
                if (!poolWorker)
                    return;
                poolWorker.state = event.state;
                console.log(`Updating state of worker ${event.id} to ${event.state}`);
                break;
            }
        }
    });
    worker.postMessage({
        label: "init",
        config: {
            executorPaths: config.executorPaths,
            postgresConfig: config.postgresConn,
            id
        }
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
    const queues = new Map();
    for (const [name, queueConfig] of Object.entries(config.queues)) {
        queues.set(name, {
            name,
            activeJobs: [],
            maxConcurrency: queueConfig.maxConcurrency,
        });
    }
    const workerPool = [];
    for (let i = 0; i < DEFAULT_WORKER_POOL_SIZE; i++) {
        const id = randomUUID();
        const worker = startWorker(config, id, workerPool);
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
const jobFromSchema = (dbJob) => {
    return {
        id: String(dbJob.id),
        worker: dbJob.worker,
        queue: dbJob.queue,
        args: JSON.parse(dbJob.args),
        insertedAt: dbJob.inserted_at,
        completedAt: dbJob.completed_at,
        state: dbJob.state === "available" ||
            dbJob.state === "completed" ||
            dbJob.state === "cancelled"
            ? dbJob.state
            : "UNKNOWN",
        attempts: dbJob.attempts,
        maxAttempts: dbJob.max_attempts,
    };
};
const createRepo = (postgresClient) => {
    const getFirstAvailableJobForQueue = async (queueName) => {
        const result = await postgresClient.query(`SELECT
					*
				FROM
					jobs
				WHERE
					(state = 'available'
						OR state = 'retryable')
					AND queue = $1::text
					AND scheduled_at <= NOW()
				LIMIT 1;`, [queueName]);
        if (!result)
            return null;
        return {
            rowCount: result.rowCount ?? 0,
            rows: result.rows.map(jobFromSchema),
        };
    };
    const markJobExecuting = async (jobId) => postgresClient.query(`UPDATE jobs SET state = 'executing', attempts = attempts +1 WHERE id = $1::int4;`, [jobId]);
    return {
        getFirstAvailableJobForQueue,
        markJobExecuting,
    };
};
const filterReadyWorkers = (p) => {
    return p.filter((worker) => {
        return worker.state === "ready";
    });
};
const mainLoop = async (queues, workerPool, postgresClient) => {
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
                console.log("reached concurrency limits");
                continue;
            }
            lastUsedWorkerIndex = await assignJobToWorker(queue, job, repo, readyWorkers, lastUsedWorkerIndex);
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
const assignJobToWorker = async (queue, job, repo, workerPool, lastUsedWorkerIndex) => {
    const [worker, newIndex] = roundRobinSelectWorker(workerPool, lastUsedWorkerIndex);
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
            const { queues, workerPool, postgresClient } = await init(event.config);
            mainLoop(queues, workerPool, postgresClient);
            break;
        }
        default:
            console.log(`Unknown event kind: ${JSON.stringify(event)}`);
    }
};
if (!parentPort)
    throw Error();
parentPort.on("message", requireLabel(handleMessage));
