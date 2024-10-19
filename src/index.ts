import { Worker } from "node:worker_threads";
import { Client, type ClientConfig as PgClientConfig } from "pg";
import type { QueueConfig } from "./Queues";
import type { Job } from "./workers/controller";

type ExecutionResult = {
	status: "completed" | "fail" | "retryable" | "executing";
};
type Executor<args extends object> = {
	name: string;
	queueName: QueueConfig["name"];
	execute:
		| ((args: args) => Promise<ExecutionResult>)
		| ((args: args, job: Job) => Promise<ExecutionResult>);
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
const DEFAULT_MAX_ATTEMPTS = 3;

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
		{
			scheduledAt = new Date(),
		}: {
			scheduledAt?: Date;
		},
	) {
		await this.pgClient.query(
			"INSERT INTO jobs (worker, queue, args, state, max_attempts, scheduled_at) VALUES ($1::text, $2::text, $3::jsonb, $4::text, $5::smallint, $6::timestamptz);",
			[
				executor.name,
				executor.queueName,
				args,
				DEFAULT_JOB_STATE,
				DEFAULT_MAX_ATTEMPTS,
				scheduledAt,
			],
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
