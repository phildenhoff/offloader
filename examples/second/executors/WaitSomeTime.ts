// @ts-check

import type { ExecutionResult, Executor } from "../../../src";

export const WaitSomeTime = (): Executor<{
	waitExtraXSeconds?: number;
}> => {
	return {
		name: "WaitSomeTime",
		queueName: "Default",
		execute: ({ waitExtraXSeconds = 0 }, job) => {
			const startedTime = new Date();
			const promise: Promise<ExecutionResult> = new Promise((resolve) => {
				setTimeout(() => {
					const { scheduledAt, insertedAt } = job;
					const timeFormatter = new Intl.DateTimeFormat("en-US", {
						hour: "numeric",
						minute: "numeric",
						second: "numeric",
						hour12: false,
					});

					console.log(
						"[WaitSomeTimeExecutor]",
						`Inserted at ${timeFormatter.format(new Date(insertedAt))}` +
							`\t ${scheduledAt ? `scheduled for ${timeFormatter.format(new Date(scheduledAt))}` : "not scheduled"}` +
							`\t started at ${timeFormatter.format(startedTime)}` +
							`\t now: ${timeFormatter.format(new Date())}`,
					);

					resolve({ status: "completed" });
				}, waitExtraXSeconds * 1000);
			});
			return promise;
		},
	};
};
