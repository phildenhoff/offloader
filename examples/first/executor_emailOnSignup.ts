import type { Executor } from "../../src/index";

export const EmailOnSignupExecutor = (): Executor<{ userId: number }> => {
	return {
		name: "EmailOnSignup",
		execute: (userId) => {
			console.log(`'Emailed' user ${userId}`);

			return Promise.resolve({ result: "success" });
		},
	};
};
