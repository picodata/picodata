import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import postcss from "postcss";
import {
  readTheme,
  renderTokensCss,
  renderFontFaces,
} from "./build-ui-kit.mjs";
import { tokens } from "../packages/ui-kit/dist/tokens.js";

const directory = new URL("../packages/ui-kit/dist/", import.meta.url);
const css = postcss.parse(
  readFileSync(new URL("tokens.css", directory), "utf8")
);
const variables = new Map();
css.walkRules(":root", (rule) => {
  rule.walkDecls((declaration) => {
    assert.ok(!variables.has(declaration.prop), declaration.prop);
    variables.set(declaration.prop, declaration.value);
  });
});

test("JSON and JS exports preserve every theme token", async () => {
  const { themeOptions } = await readTheme();
  const { palette, common, shape } = themeOptions;
  const expected = { palette, common, shape };
  assert.deepEqual(tokens, expected);
  assert.deepEqual(
    JSON.parse(readFileSync(new URL("tokens.json", directory), "utf8")),
    expected
  );
});

test("CSS exposes the ADR names, valid dimensions and normalized shadows", () => {
  assert.equal(
    variables.get("--pico-color-bg-light"),
    tokens.common.colors.bg.colorBgLight
  );
  assert.equal(
    variables.get("--pico-color-primary"),
    tokens.palette.primary.main
  );
  assert.equal(
    variables.get("--pico-color-text-primary"),
    tokens.palette.text.primary
  );
  assert.equal(
    variables.get("--pico-font-family"),
    tokens.common.variables.fontFamily
  );
  assert.equal(
    variables.get("--pico-radius-default"),
    `${tokens.shape.borderRadius}px`
  );
  assert.equal(
    variables.get("--pico-shadow-elevation-1"),
    tokens.common.variables.elevation1.replace(/\s+/g, " ").trim()
  );
  assert.equal(
    variables.get("--pico-z-index-modal"),
    String(tokens.common.zIndex.zIndexModal)
  );
});

test("tokens.css includes all font faces and references shipped font bytes", async () => {
  const { fontFaces } = await readTheme();
  const source = postcss.parse(fontFaces);
  const expected = [];
  const actual = [];
  source.walkAtRules("font-face", (rule) => expected.push(rule));
  css.walkAtRules("font-face", (rule) => actual.push(rule));
  assert.ok(expected.length > 0);
  assert.equal(actual.length, expected.length);
  for (const rule of actual) {
    rule.walkDecls("src", ({ value }) => {
      const [, file] = value.match(/url\(\.\/(fonts\/[^)]+)\)/);
      assert.deepEqual(
        readFileSync(new URL(file, directory)),
        readFileSync(new URL(`../src/assets/${file}`, import.meta.url))
      );
    });
  }
});

test("conflicting CSS names and unsupported token values fail the build", () => {
  const duplicate = structuredClone(tokens);
  duplicate.common.colors.other = { colorPrimary: "red" };
  assert.throws(() => renderTokensCss(duplicate), /Duplicate CSS token/);
  const unsupported = structuredClone(tokens);
  unsupported.common.variables.unsupported = false;
  assert.throws(() => renderTokensCss(unsupported), /Unsupported token/);
});

test("font paths cannot escape the assets directory", () => {
  assert.throws(
    () => renderFontFaces("src: url(/src/assets/fonts/../private.woff2)"),
    /Invalid font path/
  );
});
