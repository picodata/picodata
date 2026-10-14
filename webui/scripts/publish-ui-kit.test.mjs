import assert from "node:assert/strict";
import { test } from "node:test";

import { publicationConfig } from "./publish-ui-kit.mjs";

const registry = "https://nexus.example.test/repository/npm/";

test("registry URL is normalized and Yarn credentials are scoped without storing the token", () => {
  const config = publicationConfig({
    NPM_REGISTRY_URL: registry.slice(0, -1),
    NPM_REGISTRY_TOKEN: "test-secret",
  });
  assert.equal(config.registry, registry);
  assert.equal(config.token, "test-secret");
  assert.deepEqual(JSON.parse(config.yarnrc), {
    npmScopes: { picodata: { npmPublishRegistry: registry } },
    npmRegistries: {
      [registry]: {
        npmAuthToken: "${NPM_REGISTRY_TOKEN}",
        npmAlwaysAuth: true,
      },
    },
  });
  assert.ok(!config.yarnrc.includes("test-secret"));
});

test("publication requires explicit registry configuration", () => {
  for (const env of [
    {},
    { NPM_REGISTRY_URL: registry },
    { NPM_REGISTRY_TOKEN: "token" },
  ]) {
    assert.throws(
      () => publicationConfig(env),
      /requires NPM_REGISTRY_URL and NPM_REGISTRY_TOKEN/
    );
  }
  for (const url of [
    "file:///tmp/npm",
    "https://user:password@nexus.example.test/npm/",
    `${registry}?token=secret`,
    `${registry}#fragment`,
    `${registry}\nother=value`,
    "https://${REGISTRY_HOST}/npm/",
  ]) {
    assert.throws(() =>
      publicationConfig({
        NPM_REGISTRY_URL: url,
        NPM_REGISTRY_TOKEN: "token",
      })
    );
  }
});
