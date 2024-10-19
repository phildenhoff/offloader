import type { Config } from "./_type";

// We have to cast the type to be less specific because TS thinks that the
// current value is how the file will always look, even though it'll be
// modified.
const localOverrides = (await import("./dev.local")).default as Record<string, string>;

const defaultOrOverride = (key: string, defaultVal: string): string => {
  if (localOverrides && key in localOverrides && localOverrides[key] !== undefined) {
    return localOverrides[key];
  }

  return defaultVal;
}

const config: Config = {
  "PG_HOST": defaultOrOverride("PG_HOST", "localhost"),
  "PG_PORT": defaultOrOverride("PG_PORT", "5432"),
  "PG_USER": defaultOrOverride("PG_USER", "postgres"),
  "PG_PASS": defaultOrOverride("PG_PASS", "postgres"),
  "PG_OFFLOADER_DB": defaultOrOverride("PG_OFFLOADER_DB", "offloader-test")
};

export default config;
