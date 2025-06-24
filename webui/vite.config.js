import * as fs from "fs";
import * as path from "path";

import mime from "mime-types";
import react from "@vitejs/plugin-react";
import svgr from "vite-plugin-svgr";
import debug from "debug";
import { defineConfig, loadEnv } from "vite";

const currentDir = "./picodata-ui";

const walkSync = function (dir, filelist = []) {
  const files = fs.readdirSync(dir);
  let currentFileList = [...filelist];
  files.forEach((file) => {
    if (fs.statSync(dir + file).isDirectory()) {
      currentFileList = walkSync(dir + file + "/", currentFileList);
    } else {
      currentFileList.push(dir + file);
    }
  });
  return currentFileList;
};

const generateBuildFolder = (currentNamespace = "") => {
  const currentOptions = {
    namespace: currentNamespace,
    bundleName: "bundle.lua",
    entryRegExp: /main.+js$/,
    lua: "tarantool",
  };
  return {
    name: "transform-bundle",
    apply: "build",
    writeBundle(options) {
      const outputPath = options.dir;
      const {
        // bundleName,
        entryRegExp,
        // lua
      } = currentOptions;
      const buildFolder = path.relative(process.cwd(), outputPath);
      const namespaceFolder = buildFolder + "/";
      const files = walkSync(namespaceFolder);
      const filemap = {};
      for (const file of files) {
        const fileName = file.slice(namespaceFolder.length);
        const fileBody = fs.readFileSync(file, { encoding: "utf8" });
        filemap[fileName] = {
          is_entry: entryRegExp.test(fileName),
          body: fileBody,
          mime: mime.lookup(fileName),
        };
      }
      debug(path.join(currentDir, "pack-fron.lia"), "test");
      fs.writeFileSync(buildFolder + "/bundle.json", JSON.stringify(filemap), {
        encoding: "utf8",
      });
      debug("compile bundle.json");
    },
  };
};

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");

  return {
    plugins: [react(), generateBuildFolder(), svgr()],
    resolve: {
      alias: {
        modules: "/src/modules",
        shared: "/src/shared",
        assets: "/src/assets",
        styles: "/src/styles",
        store: "/src/store",
      },
    },
    server: {
      port: Number(env.PORT) || 3000,
      proxy: {
        "/api/": {
          target: env.BACKEND_URL,
          changeOrigin: true,
        },
      },
    },
  };
});
