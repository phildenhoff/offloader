
/**
 * @returns {Executor<{userId: number}>}
*/
export const EmailOnSignupExecutor = () => {
	return {
		name: "EmailOnSignup",
		execute: (userId) => {
			console.log(`'Emailed' user ${userId}`);

			return Promise.resolve({ result: "success" });
		},
	};
};
