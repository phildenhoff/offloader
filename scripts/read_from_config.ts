// Given an environment name, this script executes the JS file and prints the
// default export out to stdout in the format of bash/sh env vars.

import { readdirSync } from "node:fs";

const convertConfigToEnvVars = (config) => {
  return Object.entries(config).map(([key, value]) => {
    return `export ${key}=${value}`;
  }).join("\n");
};

const main = async () => {
  const envConfigFileName = process.argv[2];
  if (!envConfigFileName) {
    const validConfigFileNames = readdirSync("./config").filter(name => name.endsWith(".ts")).map(name => name.replace(".ts", ""));

    console.error("Usage: node scripts/read_from_config.ts <env_config_file_name>");
    console.log("Valid env config file names:");
    for (const name of validConfigFileNames) {
      console.log(`\t- ${name}`);
    }
    process.exit(1);
  }

  const config = (await import(`../config/${envConfigFileName}.ts`)).default;

  const envVars = convertConfigToEnvVars(config);
  console.log(envVars);
};

await main();
