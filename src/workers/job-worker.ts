import { parentPort } from "node:worker_threads";
import { Client as PgClient, type ClientConfig as PgClientConfig } from "pg";
import { requireLabel } from "src/messages";

let workerId: string | null;
let client: PgClient;
let executors = [];

const init = async (config: {
  executorPaths: Record<string, string>,
  postgresConfig: PgClientConfig,
  id: string
}) => {
  workerId = config.id;

  client = new PgClient(config.postgresConfig);
  await client.connect();

  executors = await Promise.all(
    [...Object.entries(config.executorPaths)].map(async ([name, path]) => {
      const executor = (await import(path))[name];
      return { name, executor };
    }),
  );

  parentPort?.postMessage({
    label: "worker_state_change",
    state: "ready",
    id: workerId
  });
};

parentPort?.on('message', requireLabel((event) => {
  switch (event.label) {
    case "init":
      // @ts-ignore
      init(event.config);
      break;
  }

}));
