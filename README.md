# Offloader 🧵🐝

> Run retryable jobs on background threads. Backed by Postgres.

Offloader allows you to schedule retryable jobs to execute using a pool of
background workers; avoid the performance hit of CPU intensive work or just
ensure that your jobs retry until they complete successfully.

## Getting Started

Add the `offloader` package to your project.

```sh
pnpm add offloader
```

You'll need to start up the worker pool, define your job executors,
and then use a `Scheduler` to add jobs to execute.

First, create the worker pool with a config.

```ts
import { initWorkerPool, createConfig } from "offloader";

const config = createConfig({
  queues: [],
  executors: {},
  postgresConn: {}
});

initWorkerPool(config);
```

Second, create your executors.

```ts
class EmailOnSignupExecutor implements IExecutor {
  // Implementation hidden
}
// (or)
// const EmailOnSignupExecutor: Executor = (...args) => { // snip };
```

Finally, create a Scheduler instance and begin scheduling jobs to execute.

```ts
import { Scheduler } from "offloader";

const scheduler = new Scheduler(config);

scheduler.enqueue(EmailOnSignupExecutor, {
  userId: 1234
});
```

## Concepts

- `Offloader` — this library.
- `Executor` — A function or class that can be used by a Worker to run a job
  with some parameters (defined when the job is enqueued).
- `Worker` — A background thread that runs `Executor`s when told to by a
  controller.
- `Controller` — Background thread that polls Postgres for jobs to run and tells
  `Workers` to execute `Executor`s with arguments when jobs are found. Controls
  queue concurrency.
