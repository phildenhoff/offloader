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
        const executor = (await import(path))[name];
        return { name, executor };
    }));
    parentPort?.postMessage({
        label: "worker_state_change",
        state: "ready",
        id: workerId
    });
};
parentPort?.on('message', requireLabel((event) => {
    switch (event.label) {
        case "init":
            // @ts-ignore
            init(event.config);
            break;
    }
}));
