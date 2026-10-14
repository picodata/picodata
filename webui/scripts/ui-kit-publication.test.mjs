import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { packUiKit } from "./check-ui-kit.mjs";
import { runYarn, yarnCli } from "./ui-kit-yarn.mjs";

test(
  "Yarn checks and publishes the staged package against a registry",
  { timeout: 120_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "ui-kit-yarn-test-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const { archive, archivePath, packageDirectory } = await packUiKit(
      directory
    );
    const expected = readFileSync(archivePath);
    let metadata;
    let tarball;
    let writes = 0;
    let requests = 0;
    let authorization;
    let failure;
    const server = createServer(async (req, res) => {
      try {
        requests++;
        assert.equal(req.headers.authorization, authorization);
        assert.ok(req.url.startsWith("/repository/npm/"));
        if (failure) {
          res.writeHead(failure);
          res.end();
        } else if (req.method === "PUT") {
          const chunks = [];
          for await (const chunk of req) chunks.push(chunk);
          metadata = JSON.parse(Buffer.concat(chunks));
          assert.equal(metadata.name, archive.name);
          tarball = Buffer.from(
            Object.values(metadata._attachments)[0].data,
            "base64"
          );
          // yarn npm publish must send the bytes that yarn pack checked.
          assert.deepEqual(tarball, expected);
          writes++;
          res.writeHead(201, { "content-type": "application/json" });
          res.end("{}");
        } else {
          assert.equal(req.method, "GET");
          if (req.url.endsWith(".tgz")) {
            res.writeHead(200, { "content-type": "application/octet-stream" });
            res.end(tarball);
          } else {
            res.writeHead(metadata ? 200 : 404, {
              "content-type": "application/json",
            });
            res.end(JSON.stringify(metadata || {}));
          }
        }
      } catch (error) {
        res.writeHead(500);
        res.end(error.message);
      }
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(() => {
      server.closeAllConnections();
      server.close();
    });
    const registry = `http://127.0.0.1:${
      server.address().port
    }/repository/npm/`;
    const run = (task, variables = {}) =>
      new Promise((resolve, reject) => {
        authorization =
          task === "publish:ui-kit" ? "Bearer test-token" : undefined;
        const child = spawn(process.execPath, [yarnCli, task], {
          cwd: fileURLToPath(new URL("../", import.meta.url)),
          env: {
            ...process.env,
            YARN_IGNORE_PATH: "1",
            YARN_ENABLE_TELEMETRY: "0",
            // HTTP is enabled only for this loopback test registry.
            YARN_UNSAFE_HTTP_WHITELIST: "127.0.0.1",
            NPM_REGISTRY_URL: registry,
            NPM_REGISTRY_TOKEN: "test-token",
            ...variables,
          },
          timeout: 30_000,
        });
        let output = "";
        child.stdout.on("data", (chunk) => {
          output += chunk;
        });
        child.stderr.on("data", (chunk) => {
          output += chunk;
        });
        child.on("error", reject);
        child.on("close", (code) => resolve({ code, output }));
      });
    const succeed = (result) => assert.equal(result.code, 0, result.output);

    succeed(await run("check:ui-kit"));
    assert.equal(writes, 0);
    succeed(await run("publish:ui-kit"));
    assert.equal(writes, 1);
    succeed(await run("check:ui-kit"));
    succeed(await run("publish:ui-kit"));
    assert.equal(writes, 1);

    const updateRemote = () => {
      runYarn(packageDirectory, ["pack", "--out", archivePath]);
      tarball = readFileSync(archivePath);
      metadata.versions[archive.version].dist = {
        integrity: `sha512-${createHash("sha512")
          .update(tarball)
          .digest("base64")}`,
        tarball: `${registry}ui-kit.tgz`,
      };
    };
    appendFileSync(join(packageDirectory, "AUTHORS"), "\nTest contributor\n");
    updateRemote();
    succeed(await run("check:ui-kit"));
    succeed(await run("publish:ui-kit"));
    assert.equal(writes, 1);

    appendFileSync(join(packageDirectory, "LICENSE"), "\nChanged license\n");
    updateRemote();
    for (const task of ["check:ui-kit", "publish:ui-kit"]) {
      const result = await run(task);
      assert.notEqual(result.code, 0);
      assert.match(result.output, /Bump the package version/);
    }
    assert.equal(writes, 1);
    failure = 403;
    for (const task of ["check:ui-kit", "publish:ui-kit"]) {
      const result = await run(task);
      assert.notEqual(result.code, 0);
      assert.match(result.output, /HTTP 403/);
    }
    const previousRequests = requests;
    let result = await run("check:ui-kit", { NPM_REGISTRY_URL: "" });
    assert.notEqual(result.code, 0);
    assert.match(result.output, /requires NPM_REGISTRY_URL/);
    result = await run("publish:ui-kit", { NPM_REGISTRY_TOKEN: "" });
    assert.notEqual(result.code, 0);
    assert.match(
      result.output,
      /requires NPM_REGISTRY_URL and NPM_REGISTRY_TOKEN/
    );
    assert.equal(requests, previousRequests);
    assert.equal(writes, 1);
  }
);
