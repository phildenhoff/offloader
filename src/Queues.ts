export type QueueConfig = {
	name: string;
	maxConcurrency: number;
};
export type JobQueue = {
	activeJobs: object[];
	name: string;
	maxConcurrency: number;
};
