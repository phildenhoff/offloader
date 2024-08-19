import { Client, type ClientConfig as PgClientConfig } from "pg";
import { Worker } from "node:worker_threads";
import type { QueueConfig } from "./Queues";

type ExecutionStatus = {
	result: "success" | "fail";
};
type Executor<args extends object> = {
	name: string;
	execute: (args: args) => Promise<ExecutionStatus>;
};
type Config = ConfigInput & {
	__brand: "Config" & never;
};
type ConfigInput = {
	queues: QueueConfig[];
	executors: {
		[key: string]: string;
	};
	postgresConn: PgClientConfig;
};

class Scheduler {
	pgClient: Client;
	connected: Promise<boolean>;

	constructor(config: Config) {
		this.pgClient = new Client(config.postgresConn);
		this.connected = new Promise((resolve) => {
			this.pgClient
				.connect()
				.then(() => resolve(true))
				.catch(() => resolve(false));
		});
	}

	enqueue<Args extends object, TExec extends Executor<Args>>(
		executor: TExec,
		args: Args,
	) {
		console.log(
			`Scheduling ${executor.name} to run with args ${JSON.stringify(args)}`,
		);
	}
}

const createConfig = (config: ConfigInput): Config => {
	return config as Config;
};
const initWorkerPool = (config: Config) => {
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

export type { Executor };
