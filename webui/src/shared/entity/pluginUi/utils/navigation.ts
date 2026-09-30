import { ServerPluginUiPageType } from "../types";

export function getPluginUiPageKey(
  page: Pick<ServerPluginUiPageType, "plugin" | "slug" | "version" | "entry">
) {
  return JSON.stringify([page.plugin, page.slug, page.version, page.entry]);
}

export function getPluginUiPagePath(
  page: Pick<ServerPluginUiPageType, "plugin" | "slug">
) {
  return `/plugin/${encodeURIComponent(page.plugin)}/${encodeURIComponent(
    page.slug
  )}`;
}

export function getPluginUiAssetBase(
  page: Pick<ServerPluginUiPageType, "plugin" | "version">
) {
  return `/plugin-ui/${encodeURIComponent(page.plugin)}/${encodeURIComponent(
    page.version
  )}`;
}

export function groupPluginUiPages(pages: ServerPluginUiPageType[]) {
  const groups = new Map<string, ServerPluginUiPageType[]>();
  pages.forEach((page) => {
    const group = groups.get(page.plugin);
    if (group) group.push(page);
    else groups.set(page.plugin, [page]);
  });
  return [...groups].sort(([a], [b]) => a.localeCompare(b));
}
