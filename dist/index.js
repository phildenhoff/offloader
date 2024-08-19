import { Client } from "pg";
import { Worker } from "node:worker_threads";
class Scheduler {
    pgClient;
    connected;
    constructor(config) {
        this.pgClient = new Client(config.postgresConn);
        this.connected = new Promise((resolve) => {
            this.pgClient
                .connect()
                .then(() => resolve(true))
                .catch(() => resolve(false));
        });
    }
    enqueue(executor, args) {
        console.log(`Scheduling ${executor.name} to run with args ${JSON.stringify(args)}`);
    }
}
const createConfig = (config) => {
    return config;
};
const initWorkerPool = (config) => {
    const controller = new Worker(new URL("../dist/workers/controller.js", import.meta.url));
    controller.postMessage({
        label: "init",
        config: {
            queues: config.queues,
            executorPaths: config.executors,
            postgresConn: config.postgresConn,
        },
    });
};
export { initWorkerPool, createConfig, Scheduler };
