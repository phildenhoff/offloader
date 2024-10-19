// @ts-check
import { Logger } from "./logger";

/**
 * @returns {import('../../src/index').Executor<{userId: number}>}
 */
export const EmailOnSignup = () => {
	return {
		name: "EmailOnSignup",
		queueName: "Default",
		execute: (userId) => {
			const logger = new Logger();
			logger.log(
				"EmailOnSignupExecutor",
				`'Emailed' user ${JSON.stringify(userId)}`,
			);
			logger.log(
				"system",
				"offloader works! You can end the process now with Ctrl+c",
			);

			return Promise.resolve({ status: "completed" });
		},
	};
};
