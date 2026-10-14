import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const { packageManager } = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8")
);
const version = /^yarn@(\d+\.\d+\.\d+)$/.exec(packageManager)?.[1];
if (!version) throw new Error("webui/package.json must pin a Yarn release");
export const yarnCli = fileURLToPath(
  new URL(`../.yarn/releases/yarn-${version}.cjs`, import.meta.url)
);

export function runYarn(directory, args, options = {}) {
  return execFileSync(process.execPath, [yarnCli, ...args], {
    cwd: directory,
    encoding: "utf8",
    ...options,
    env: { ...process.env, YARN_IGNORE_PATH: "1", ...options.env },
  });
}
