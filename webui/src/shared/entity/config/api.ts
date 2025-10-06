import { defaultConfig } from "./default";
import { WebUIConfig } from "./types";

export const configURL = "/api/v1/config";

let initialFetchDone = false;

// Typically, the config will already be available on window
// via the inline script in index.html
// but we still fetch just in-case it's not
// and fallback to defaults if all fails
export const getConfig = (): Promise<WebUIConfig> =>
  initialFetchDone || !window.appConfig
    ? fetch(configURL, { priority: "high" })
        .then((r) => r.json())
        .catch(() => window.appConfig || defaultConfig)
        .finally(() => (initialFetchDone = true))
    : window.appConfig;

export const configSync = defaultConfig;

window.appConfig?.then((c) => {
  for (const key in c) {
    const k = key as keyof WebUIConfig;
    configSync[k] = c[k];
  }

  initialFetchDone = true;
});

export type * from "./types";
