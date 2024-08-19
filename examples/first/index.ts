import { resolve } from "node:path";
import { initWorkerPool, createConfig, Scheduler } from "../../src/index";
import { EmailOnSignup } from "./executor_emailOnSignup";
import { Logger } from "./logger";

const logger = new Logger();

const config = createConfig({
	queues: [
		{
			name: "Default",
			maxConcurrency: 2,
		},
	],
	executors: {
		EmailOnSignup: new URL(
			"./executor_emailOnSignup.js",
			import.meta.url,
		).toString(),
	},
	postgresConn: {
		user: "postgres",
		password: "postgres",
		host: "127.0.0.1",
		port: 5432,
		database: "offloader-test",
		ssl: false,
	},
});

initWorkerPool(config);

const scheduler = new Scheduler(config, logger);

scheduler.enqueue(EmailOnSignup(), {
	userId: 1234,
});
