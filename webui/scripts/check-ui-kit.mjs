import { createHash } from "node:crypto";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { Parser } from "tar";
import { buildUiKit } from "./build-ui-kit.mjs";
import { runYarn } from "./ui-kit-yarn.mjs";

export function registryUrl(value) {
  const url = new URL(value);
  if (
    !["https:", "http:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    /[\s;#${}]/.test(value)
  ) {
    throw new Error(
      "NPM_REGISTRY_URL must be an HTTP(S) registry URL without credentials, query or fragment"
    );
  }
  if (!url.pathname.endsWith("/")) url.pathname += "/";
  return url.href;
}

export function preparePackage(directory) {
  const packageDirectory = new URL("../packages/ui-kit/", import.meta.url);
  mkdirSync(directory, { recursive: true });
  for (const file of ["package.json", "index.d.ts", "README.md", "dist"]) {
    cpSync(new URL(file, packageDirectory), join(directory, file), {
      recursive: true,
    });
  }
  for (const file of ["AUTHORS", "LICENSE"]) {
    cpSync(new URL(`../../${file}`, import.meta.url), join(directory, file));
  }
}

export async function packUiKit(directory) {
  await buildUiKit();
  const packageDirectory = join(directory, "package");
  preparePackage(packageDirectory);
  // Keep the staged package independent of the host workspace and user project.
  writeFileSync(join(packageDirectory, "yarn.lock"), "");
  // Yarn publication needs a resolved project; ui-kit has no dependencies.
  runYarn(packageDirectory, ["install", "--mode=skip-build"], {
    env: {
      YARN_ENABLE_IMMUTABLE_INSTALLS: "false",
      YARN_ENABLE_NETWORK: "false",
    },
  });
  const archivePath = join(directory, "package.tgz");
  runYarn(packageDirectory, ["pack", "--out", archivePath]);
  const { name, version } = JSON.parse(
    readFileSync(join(packageDirectory, "package.json"), "utf8")
  );
  const contents = readFileSync(archivePath);
  const archive = {
    name,
    version,
    integrity: `sha512-${createHash("sha512")
      .update(contents)
      .digest("base64")}`,
    shasum: createHash("sha1").update(contents).digest("hex"),
  };
  return { archive, archivePath, packageDirectory };
}

export function packageContents(tarball) {
  return new Promise((resolve, reject) => {
    const files = new Map();
    const parser = new Parser({ strict: true });
    parser.on("error", reject);
    parser.on("entry", (entry) => {
      if (entry.type === "Directory") {
        entry.resume();
        return;
      }
      if (
        entry.type !== "File" ||
        !entry.path.startsWith("package/") ||
        entry.path.split("/").some((part) => part === "." || part === "..") ||
        entry.path.includes("\\") ||
        files.has(entry.path)
      ) {
        reject(new Error(`Unsupported package entry: ${entry.path}`));
        entry.resume();
        return;
      }
      // Track AUTHORS too so duplicate entries are rejected.
      files.set(entry.path, null);
      const hash = createHash("sha256");
      entry.on("data", (chunk) => hash.update(chunk));
      entry.on("error", reject);
      entry.on("end", () => {
        files.set(entry.path, [entry.mode & 0o111, hash.digest("hex")]);
      });
      entry.resume();
    });
    parser.on("end", () => {
      files.delete("package/AUTHORS");
      resolve(
        JSON.stringify([...files].sort(([a], [b]) => a.localeCompare(b)))
      );
    });
    parser.end(tarball);
  });
}

// The token is optional: reading the registry may be anonymous.
export async function needsPublication(registry, token, archive, archivePath) {
  const headers = token ? { Authorization: `Bearer ${token}` } : {};
  const response = await fetch(
    `${registry}${encodeURIComponent(archive.name)}`,
    {
      headers,
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
    }
  );
  if (response.status === 404) return true;
  if (!response.ok) {
    throw new Error(`Package registry returned HTTP ${response.status}`);
  }
  const metadata = await response.json();
  const published = metadata.versions?.[archive.version];
  if (!published) return true;
  const unchanged = published.dist?.integrity
    ? published.dist.integrity === archive.integrity
    : Boolean(published.dist?.shasum) &&
      published.dist.shasum === archive.shasum;
  if (unchanged) return false;

  const base = new URL(registry);
  const tarballUrl =
    published.dist?.tarball && new URL(published.dist.tarball, base);
  if (
    !tarballUrl ||
    tarballUrl.origin !== base.origin ||
    !tarballUrl.pathname.startsWith(base.pathname) ||
    tarballUrl.username ||
    tarballUrl.password
  ) {
    throw new Error(
      "Published package tarball must be inside the configured registry"
    );
  }
  const tarballResponse = await fetch(tarballUrl.href, {
    headers,
    redirect: "error",
    signal: AbortSignal.timeout(30_000),
  });
  if (!tarballResponse.ok) {
    throw new Error(`Package tarball returned HTTP ${tarballResponse.status}`);
  }
  const publishedContents = await packageContents(
    Buffer.from(await tarballResponse.arrayBuffer())
  );
  const localContents = await packageContents(readFileSync(archivePath));
  if (publishedContents !== localContents) {
    throw new Error(
      `${archive.name}@${archive.version} already exists with different contents. Bump the package version.`
    );
  }
  return false;
}

async function main() {
  if (!process.env.NPM_REGISTRY_URL) {
    throw new Error("The check requires NPM_REGISTRY_URL");
  }
  const registry = registryUrl(process.env.NPM_REGISTRY_URL);
  const directory = mkdtempSync(join(tmpdir(), "picodata-ui-kit-"));
  try {
    const { archive, archivePath } = await packUiKit(directory);
    const id = `${archive.name}@${archive.version}`;
    console.info(
      (await needsPublication(registry, undefined, archive, archivePath))
        ? `${id} is not published yet and will be published on the next release`
        : `${id} is already published with equivalent contents (excluding AUTHORS)`
    );
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
