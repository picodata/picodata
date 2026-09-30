import { PluginUiPageType } from "shared/entity/pluginUi";
import { getPluginUiAssetBase } from "shared/entity/pluginUi/utils";

import { PluginPageLoadError, PluginPageErrorStage } from "./errors";

const encodeCustomElementNamePart = (value: string) =>
  Array.from(new TextEncoder().encode(value), (byte) =>
    byte.toString(16).padStart(2, "0")
  ).join("");

export async function loadPluginPage(page: PluginUiPageType) {
  let stage: PluginPageErrorStage = "entry";
  try {
    const tag = `picodata-plugin-${encodeCustomElementNamePart(
      page.plugin
    )}-${encodeCustomElementNamePart(page.slug)}-${encodeCustomElementNamePart(
      page.version
    )}`;
    if (customElements.get(tag)) return tag;
    const parts = page.entry.split("/").filter((part) => part !== ".");
    if (
      !parts.length ||
      parts.some((part) => !part || part === ".." || part.includes("\\"))
    ) {
      throw new Error(
        `Invalid plugin entry path: ${JSON.stringify(page.entry)}`
      );
    }
    const url = `${getPluginUiAssetBase(page)}/${parts
      .map(encodeURIComponent)
      .join("/")}`;
    stage = "import";
    const module = await import(/* @vite-ignore */ url);
    stage = "registration";
    const Page = module.default;
    if (
      typeof Page !== "function" ||
      !(Page.prototype instanceof HTMLElement)
    ) {
      throw new Error(
        "Plugin entry must export an HTMLElement class as default"
      );
    }
    if (!customElements.get(tag)) {
      const PageClass = Page as CustomElementConstructor;
      customElements.define(tag, class extends PageClass {});
    }
    return tag;
  } catch (error) {
    throw new PluginPageLoadError(stage, error);
  }
}
