import { Client } from "pg";
import { Worker } from "node:worker_threads";
const JOB_STATES = {
	Available: "available",
	Executing: "executing",
};
const DEFAULT_JOB_STATE = JOB_STATES.Available;
class Scheduler {
	pgClient;
	connected;
	logger;
	constructor(config, logger) {
		this.pgClient = new Client(config.postgresConn);
		this.connected = new Promise((resolve) => {
			this.pgClient
				.connect()
				.then(() => resolve(true))
				.catch(() => resolve(false));
		});
		this.logger = logger ?? console;
	}
	async enqueue(executor, args) {
		await this.pgClient.query(
			"INSERT INTO jobs (worker, queue, args, state) VALUES ($1::text, $2::text, $3::jsonb, $4::text);",
			[executor.name, executor.queueName, args, DEFAULT_JOB_STATE],
		);
	}
}
const createConfig = (config) => {
	return config;
};
const initWorkerPool = (config) => {
	const controller = new Worker(
		new URL("../dist/workers/controller.js", import.meta.url),
	);
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
