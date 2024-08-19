import { parentPort } from "node:worker_threads";
import { Client as PgClient } from "pg";
import { requireLabel } from "src/messages";
let workerId;
let client;
let executors = [];
const init = async (config) => {
    workerId = config.id;
    client = new PgClient(config.postgresConfig);
    await client.connect();
    executors = await Promise.all([...Object.entries(config.executorPaths)].map(async ([name, path]) => {
        const executor = (await import(path))[name]();
        return executor;
    }));
    parentPort?.postMessage({
        label: "worker_state_change",
        state: "ready",
        id: workerId,
    });
    console.log("[WORKER]", `Worker ${workerId} ready to execute jobs`);
};
const executeJob = async (job) => {
    const args = job.args;
    const jobExecutor = executors.find((executor) => executor.name === job.worker);
    if (!jobExecutor) {
        return Promise.resolve({ status: "retryable" });
    }
    const status = await jobExecutor.execute(args);
    return status;
};
const reportJobStatus = (result, jobId, jobQueue) => {
    parentPort?.postMessage({
        label: "job_state_change",
        id: jobId,
        queue: jobQueue,
        status: result.status,
    });
};
parentPort?.on("message", requireLabel(async (event) => {
    switch (event.label) {
        case "init":
            // @ts-ignore
            init(event.config);
            break;
        case "execute": {
            const job = event.job;
            const status = await executeJob(job);
            reportJobStatus(status, job.id, job.queue);
        }
    }
}));
