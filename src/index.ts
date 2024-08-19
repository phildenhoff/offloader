type ExecutionStatus = {
	result: "success" | "fail";
};
type Executor<args extends object> = {
	name: string;
	execute: (args: args) => Promise<ExecutionStatus>;
};

export type { Executor };
