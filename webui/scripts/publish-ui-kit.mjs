import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { needsPublication, packUiKit, registryUrl } from "./check-ui-kit.mjs";
import { runYarn } from "./ui-kit-yarn.mjs";

export function publicationConfig({ NPM_REGISTRY_URL, NPM_REGISTRY_TOKEN }) {
  if (!NPM_REGISTRY_URL || !NPM_REGISTRY_TOKEN) {
    throw new Error(
      "Publication requires NPM_REGISTRY_URL and NPM_REGISTRY_TOKEN"
    );
  }
  const registry = registryUrl(NPM_REGISTRY_URL);
  return {
    registry,
    token: NPM_REGISTRY_TOKEN,
    yarnrc: JSON.stringify({
      npmScopes: { picodata: { npmPublishRegistry: registry } },
      npmRegistries: {
        [registry]: {
          npmAuthToken: "${NPM_REGISTRY_TOKEN}",
          npmAlwaysAuth: true,
        },
      },
    }),
  };
}

async function main() {
  const { registry, token, yarnrc } = publicationConfig(process.env);
  const directory = mkdtempSync(join(tmpdir(), "picodata-ui-kit-"));
  try {
    const { archive, archivePath, packageDirectory } = await packUiKit(
      directory
    );
    if (!(await needsPublication(registry, token, archive, archivePath))) {
      console.info(
        `${archive.name}@${archive.version} is already published with equivalent contents (excluding AUTHORS)`
      );
      return;
    }
    writeFileSync(join(packageDirectory, ".yarnrc.yml"), yarnrc, {
      mode: 0o600,
    });
    runYarn(packageDirectory, ["npm", "publish"], { stdio: "inherit" });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
