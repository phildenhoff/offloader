import { initWorkerPool, createConfig, Scheduler } from "../../src/index";
import { WaitSomeTime } from "./executors/WaitSomeTime";

const config = createConfig({
	queues: [
		{
			name: "Default",
			maxConcurrency: 2,
		},
	],
	executors: {
		WaitSomeTime: new URL(
			"./executors/WaitSomeTime.ts",
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

const scheduler = new Scheduler(config);

scheduler.enqueue(
	WaitSomeTime(),
	{},
	{
		scheduledAt: new Date(Date.now() + 10_00),
	},
);
console.log("Job enqueued... waiting for it to run...");

// You can also schedule many jobs to run at the same time, but if they're slow,
// you'll be rate limited by the number of workers you have in your pool.
scheduler.enqueue(
	WaitSomeTime(),
	{
		waitExtraXSeconds: 10,
	},
	{
		scheduledAt: new Date(Date.now() + 10_00),
	},
);
scheduler.enqueue(
	WaitSomeTime(),
	{
		waitExtraXSeconds: 10,
	},
	{
		scheduledAt: new Date(Date.now() + 10_00),
	},
);
scheduler.enqueue(
	WaitSomeTime(),
	{
		waitExtraXSeconds: 10,
	},
	{
		scheduledAt: new Date(Date.now() + 10_00),
	},
);
