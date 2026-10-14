import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { create } from "tar";

import {
  needsPublication,
  preparePackage,
  packageContents,
  registryUrl,
} from "./check-ui-kit.mjs";

const archive = {
  name: "@picodata/ui-kit",
  version: "0.1.0",
  integrity: "sha512-test",
  shasum: "test-shasum",
};
const registry = "https://nexus.example.test/repository/npm/";

test("new package is published using the configured registry and token", async (t) => {
  t.mock.method(globalThis, "fetch", async (url, options) => {
    assert.equal(url, `${registry}%40picodata%2Fui-kit`);
    assert.equal(options.headers.Authorization, "Bearer test-token");
    assert.equal(options.redirect, "error");
    return new Response(null, { status: 404 });
  });
  assert.equal(await needsPublication(registry, "test-token", archive), true);
});

test("registry URL is normalized and must not carry credentials", () => {
  assert.equal(registryUrl(registry.slice(0, -1)), registry);
  for (const url of [
    "file:///tmp/npm",
    "https://user:password@nexus.example.test/npm/",
    `${registry}?token=secret`,
    `${registry}#fragment`,
  ]) {
    assert.throws(() => registryUrl(url), /HTTP\(S\) registry URL/);
  }
});

test("new version is published", async (t) => {
  t.mock.method(globalThis, "fetch", async () =>
    Response.json({ versions: {} })
  );
  assert.equal(await needsPublication(registry, "test-token", archive), true);
});

test("identical version is skipped on a repeated release", async (t) => {
  t.mock.method(globalThis, "fetch", async () =>
    Response.json({
      versions: { "0.1.0": { dist: { integrity: archive.integrity } } },
    })
  );
  assert.equal(await needsPublication(registry, "test-token", archive), false);
});

test("registry metadata with only a shasum is supported", async (t) => {
  t.mock.method(globalThis, "fetch", async () =>
    Response.json({
      versions: { "0.1.0": { dist: { shasum: archive.shasum } } },
    })
  );
  assert.equal(await needsPublication(registry, "test-token", archive), false);
});

test("publication stages root legal files together with the package assets", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "ui-kit-package-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  preparePackage(directory);
  assert.deepEqual(
    readdirSync(directory).sort(),
    [
      "AUTHORS",
      "LICENSE",
      "README.md",
      "dist",
      "index.d.ts",
      "package.json",
    ].sort()
  );
  for (const file of ["AUTHORS", "LICENSE"]) {
    assert.deepEqual(
      readFileSync(join(directory, file)),
      readFileSync(new URL(`../../${file}`, import.meta.url))
    );
  }
});

const packageFiles = {
  "package/AUTHORS": "Original authors\n",
  "package/LICENSE": "License\n",
  "package/index.d.ts": "export interface WebuiSdk {}\n",
  "package/dist/tokens.css": ":root { --pico-color-primary: orange; }\n",
  "package/README.md": "SDK documentation\n",
  "package/dist/AUTHORS": "Nested file must not be ignored\n",
};

async function packFiles(t, files) {
  const directory = mkdtempSync(join(tmpdir(), "ui-kit-archive-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const [file, contents] of Object.entries(files)) {
    mkdirSync(dirname(join(directory, file)), { recursive: true });
    writeFileSync(join(directory, file), contents);
  }
  const path = join(directory, "package.tgz");
  await create({ cwd: directory, file: path, gzip: true }, Object.keys(files));
  return path;
}

function mockPublishedArchive(
  t,
  bytes,
  tarball = `${registry}@picodata/ui-kit/-/ui-kit-0.1.0.tgz`
) {
  return t.mock.method(globalThis, "fetch", async (url, options) => {
    assert.equal(options.headers.Authorization, "Bearer test-token");
    assert.equal(options.redirect, "error");
    if (url === `${registry}%40picodata%2Fui-kit`) {
      return Response.json({
        versions: { "0.1.0": { dist: { integrity: "sha512-other", tarball } } },
      });
    }
    assert.equal(url, tarball);
    return new Response(bytes);
  });
}

test("AUTHORS-only changes skip publication without replacing the published package", async (t) => {
  const old = await packFiles(t, packageFiles);
  const current = await packFiles(t, {
    ...packageFiles,
    "package/AUTHORS": "New contributor\n",
  });
  const fetch = mockPublishedArchive(t, readFileSync(old));
  assert.equal(
    await needsPublication(registry, "test-token", archive, current),
    false
  );
  assert.equal(fetch.mock.callCount(), 2);
});

test("archive entry order does not require a new version", async (t) => {
  const old = await packFiles(t, packageFiles);
  const current = await packFiles(
    t,
    Object.fromEntries(Object.entries(packageFiles).reverse())
  );
  mockPublishedArchive(t, readFileSync(old));
  assert.equal(
    await needsPublication(registry, "test-token", archive, current),
    false
  );
});

test("anonymous lookup compares contents without sending credentials", async (t) => {
  const old = await packFiles(t, packageFiles);
  const current = await packFiles(t, {
    ...packageFiles,
    "package/index.d.ts": "Changed content\n",
  });
  const tarball = `${registry}@picodata/ui-kit/-/ui-kit-0.1.0.tgz`;
  const fetch = t.mock.method(globalThis, "fetch", async (url, options) => {
    assert.deepEqual(options.headers, {});
    assert.equal(options.redirect, "error");
    if (url === `${registry}%40picodata%2Fui-kit`) {
      return Response.json({
        versions: { "0.1.0": { dist: { integrity: "sha512-other", tarball } } },
      });
    }
    assert.equal(url, tarball);
    return new Response(readFileSync(old));
  });
  await assert.rejects(
    needsPublication(registry, undefined, archive, current),
    /Bump/
  );
  assert.equal(fetch.mock.callCount(), 2);
});

for (const file of [
  "LICENSE",
  "index.d.ts",
  "dist/tokens.css",
  "README.md",
  "dist/AUTHORS",
]) {
  test(`changes to ${file} still require a version bump`, async (t) => {
    const old = await packFiles(t, packageFiles);
    const current = await packFiles(t, {
      ...packageFiles,
      "package/AUTHORS": "New contributor\n",
      [`package/${file}`]: "Changed content\n",
    });
    mockPublishedArchive(t, readFileSync(old));
    await assert.rejects(
      needsPublication(registry, "test-token", archive, current),
      /Bump/
    );
  });
}

for (const added of [true, false]) {
  test(`${
    added ? "adding" : "removing"
  } a file requires a version bump`, async (t) => {
    const old = await packFiles(t, packageFiles);
    const files = { ...packageFiles };
    if (added) files["package/dist/new.js"] = "export const value = 1";
    else delete files["package/dist/tokens.css"];
    const current = await packFiles(t, files);
    mockPublishedArchive(t, readFileSync(old));
    await assert.rejects(
      needsPublication(registry, "test-token", archive, current),
      /Bump/
    );
  });
}

test("a tarball outside the configured registry is rejected before sending credentials", async (t) => {
  const fetch = mockPublishedArchive(
    t,
    "",
    "https://other.example.test/package.tgz"
  );
  await assert.rejects(
    needsPublication(registry, "test-token", archive),
    /inside the configured registry/
  );
  assert.equal(fetch.mock.callCount(), 1);
});

test("tarball download errors fail publication", async (t) => {
  const tarball = `${registry}ui-kit.tgz`;
  t.mock.method(globalThis, "fetch", async (url) =>
    url === tarball
      ? new Response(null, { status: 403 })
      : Response.json({ versions: { "0.1.0": { dist: { tarball } } } })
  );
  await assert.rejects(
    needsPublication(registry, "test-token", archive),
    /tarball returned HTTP 403/
  );
});

test("invalid archives fail comparison", async () => {
  await assert.rejects(packageContents(Buffer.from("not a tar archive")));
});

for (const status of [401, 403, 500]) {
  test(`HTTP ${status} is not treated as a missing package`, async (t) => {
    t.mock.method(
      globalThis,
      "fetch",
      async () => new Response(null, { status })
    );
    await assert.rejects(
      needsPublication(registry, "test-token", archive),
      new RegExp(`HTTP ${status}`)
    );
  });
}
