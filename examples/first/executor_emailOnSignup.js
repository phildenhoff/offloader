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

			return Promise.resolve({ status: "completed" });
		},
	};
};
