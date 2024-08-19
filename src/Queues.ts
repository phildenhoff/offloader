import { Job } from "./workers/controller";

export type QueueConfig = {
	name: string;
	maxConcurrency: number;
};
export type JobQueueActiveJob = {
	id: Job["id"];
	startedAt: Date;
};
export type JobQueue = {
	activeJobs: JobQueueActiveJob[];
	name: string;
	maxConcurrency: number;
};
