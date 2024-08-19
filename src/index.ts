import { Client, type ClientConfig as PgClientConfig } from "pg";
import { Worker } from "node:worker_threads";
import type { QueueConfig } from "./Queues";

type ExecutionResult = {
	status: "completed" | "fail" | "retryable" | "executing";
};
type Executor<args extends object> = {
	name: string;
	queueName: QueueConfig["name"];
	execute: (args: args) => Promise<ExecutionResult>;
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

const JOB_STATES = {
	Available: "available",
	Executing: "executing",
} as const;
const DEFAULT_JOB_STATE = JOB_STATES.Available;

class Scheduler {
	pgClient: Client;
	connected: Promise<boolean>;
	logger: ILogger;

	constructor(config: Config, logger?: ILogger) {
		this.pgClient = new Client(config.postgresConn);
		this.connected = new Promise((resolve) => {
			this.pgClient
				.connect()
				.then(() => resolve(true))
				.catch(() => resolve(false));
		});

		this.logger = logger ?? console;
	}

	async enqueue<Args extends object, TExec extends Executor<Args>>(
		executor: TExec,
		args: Args,
	) {
		this.logger.log(
			"Scheduler",
			`Scheduling ${executor.name} to run with args ${JSON.stringify(args)}`,
		);
		await this.pgClient.query(
			"INSERT INTO jobs (worker, queue, args, state) VALUES ($1::text, $2::text, $3::jsonb, $4::text);",
			[executor.name, executor.queueName, args, DEFAULT_JOB_STATE],
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

export type { ExecutionResult, Executor };
