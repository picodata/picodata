import {
  mkdirSync,
  readFileSync,
  readdirSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";

const webuiDirectory = fileURLToPath(new URL("../", import.meta.url));
const outputDirectory = join(webuiDirectory, "packages/ui-kit/dist");

export async function readTheme() {
  const file = join(webuiDirectory, "src/shared/theme/theme.ts");
  const source = ts.createSourceFile(
    file,
    readFileSync(file, "utf8"),
    ts.ScriptTarget.Latest,
    true
  );
  const declarations = source.statements
    .filter(ts.isVariableStatement)
    .flatMap((statement) => statement.declarationList.declarations)
    .filter((declaration) =>
      ["themeOptions", "fontFaces"].includes(declaration.name.getText(source))
    );
  if (declarations.length !== 2) {
    throw new Error("theme.ts must declare themeOptions and fontFaces");
  }
  const { outputText } = ts.transpileModule(
    declarations
      .map((declaration) => `export const ${declaration.getText(source)};`)
      .join("\n"),
    {
      compilerOptions: {
        module: ts.ModuleKind.ESNext,
        target: ts.ScriptTarget.ES2020,
      },
    }
  );
  return import(
    `data:text/javascript;base64,${Buffer.from(outputText).toString("base64")}`
  );
}

const kebab = (key) =>
  key
    .replace(/^\$/, "")
    .replace(/([a-z])([A-Z0-9])/g, "$1-$2")
    .replace(/([0-9])([A-Za-z])/g, "$1-$2")
    .toLowerCase();

export function flattenTokens(value, path = [], leaves = []) {
  for (const [key, child] of Object.entries(value)) {
    const next = [...path, key];
    if (child && typeof child === "object" && !Array.isArray(child)) {
      flattenTokens(child, next, leaves);
    } else if (typeof child === "string" || typeof child === "number") {
      leaves.push([next, child]);
    } else {
      throw new Error(`Unsupported token: ${next.join(".")}`);
    }
  }
  return leaves;
}

export function renderTokensCss(tokens) {
  const variables = new Map();
  const add = (name, value) => {
    if (variables.has(name)) throw new Error(`Duplicate CSS token: ${name}`);
    variables.set(name, String(value).replace(/\s+/g, " ").trim());
  };
  for (const [path, value] of flattenTokens(tokens)) {
    const key = path.at(-1);
    let name;
    let cssValue = value;
    if (path[0] === "palette") {
      name = path.map(kebab).join("-");
    } else if (path[0] === "shape") {
      if (key !== "borderRadius")
        throw new Error(`Unsupported shape token: ${key}`);
      name = "radius-default";
      cssValue = typeof value === "number" ? `${value}px` : value;
    } else {
      name = /^(elevation)/.test(key) ? `shadow-${kebab(key)}` : kebab(key);
    }
    add(`--pico-${name}`, cssValue);
  }
  add("--pico-color-text-primary", tokens.palette.text.primary);
  add("--pico-color-text-secondary", tokens.palette.text.secondary);
  return `:root {\n${[...variables]
    .map(([name, value]) => `  ${name}: ${value};`)
    .join("\n")}\n}\n`;
}

export function renderFontFaces(fontFaces) {
  const files = new Set();
  const css = fontFaces.replace(
    /url\(\s*(['"]?)\/src\/assets\/(fonts\/[^'"\s)]+)\1\s*\)/g,
    (_, quote, file) => {
      if (file.split("/").includes(".."))
        throw new Error(`Invalid font path: ${file}`);
      files.add(file);
      return `url(${quote}./${file}${quote})`;
    }
  );
  if (!files.size) throw new Error("No font files found in theme.ts");
  return { css: css.trim(), files: [...files].sort() };
}

function typeLiteral(value) {
  if (typeof value !== "object") return JSON.stringify(value);
  return `{\n${Object.entries(value)
    .map(
      ([key, child]) =>
        `readonly ${JSON.stringify(key)}: ${typeLiteral(child)};`
    )
    .join("\n")}\n}`;
}

export async function buildUiKit() {
  const { themeOptions, fontFaces } = await readTheme();
  const { palette, common, shape } = themeOptions;
  const tokens = { palette, common, shape };
  const fonts = renderFontFaces(fontFaces);
  const json = JSON.stringify(tokens, null, 2);
  const css = renderTokensCss(tokens);
  const header = "Generated from webui/src/shared/theme/theme.ts. Do not edit.";
  const expectedFiles = new Set();
  const write = (file, content) => {
    const target = join(outputDirectory, file);
    expectedFiles.add(target);
    mkdirSync(dirname(target), { recursive: true });
    try {
      if (readFileSync(target).equals(Buffer.from(content))) return;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    writeFileSync(target, content);
  };
  write("tokens.css", `/* ${header} */\n${fonts.css}\n\n${css}`);
  write("tokens.json", `${json}\n`);
  write(
    "tokens.ts",
    `// ${header}\nexport const tokens = ${json} as const;\nexport type Tokens = typeof tokens;\n`
  );
  write("tokens.js", `// ${header}\nexport const tokens = ${json};\n`);
  write(
    "tokens.d.ts",
    `// ${header}\nexport declare const tokens: ${typeLiteral(
      tokens
    )};\nexport type Tokens = typeof tokens;\n`
  );
  for (const file of fonts.files) {
    write(file, readFileSync(join(webuiDirectory, "src/assets", file)));
  }
  const removeStaleFiles = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const file = join(directory, entry.name);
      if (entry.isDirectory()) removeStaleFiles(file);
      else if (!expectedFiles.has(file)) unlinkSync(file);
    }
  };
  removeStaleFiles(outputDirectory);
  return tokens;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  buildUiKit().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
