import { parentPort } from "node:worker_threads";
import { Client as PgClient, type ClientConfig as PgClientConfig } from "pg";
import { requireLabel } from "src/messages";
import type { Job } from "./controller";
import type { ExecutionResult, Executor } from "src";

let workerId: string | null;
let client: PgClient;
let executors: Executor<object>[] = [];

const init = async (config: {
	executorPaths: Record<string, string>;
	postgresConfig: PgClientConfig;
	id: string;
}) => {
	workerId = config.id;

	client = new PgClient(config.postgresConfig);
	await client.connect();

	executors = await Promise.all(
		[...Object.entries(config.executorPaths)].map(async ([name, path]) => {
			const executor = (await import(path))[name]() as Executor<object>;
			return executor;
		}),
	);

	parentPort?.postMessage({
		label: "worker_state_change",
		state: "ready",
		id: workerId,
	});
};

const executeJob = async (job: Job): Promise<ExecutionResult> => {
	const args = job.args as object;
	type JobExecutor = Executor<typeof args>;

	const jobExecutor: JobExecutor | undefined = executors.find(
		(executor) => executor.name === job.worker,
	);
	if (!jobExecutor) {
		return Promise.resolve({ status: "retryable" });
	}

	const status = await jobExecutor.execute(args);

	return status;
};

const reportJobStatus = (
	result: ExecutionResult,
	jobId: Job["id"],
	jobQueue: Job["queue"],
) => {
	parentPort?.postMessage({
		label: "job_state_change",
		id: jobId,
		queue: jobQueue,
		status: result.status,
	});
};

parentPort?.on(
	"message",
	requireLabel(async (event) => {
		switch (event.label) {
			case "init":
				// @ts-ignore
				init(event.config);
				break;
			case "execute": {
				const job = event.job as Job;
				const status = await executeJob(job);
				reportJobStatus(status, job.id, job.queue);
			}
		}
	}),
);
