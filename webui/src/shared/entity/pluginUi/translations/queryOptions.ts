import { getPluginUiAssetBase } from "../utils";

import { getPluginUiTranslations } from "./api";
import { PLUGIN_UI_TRANSLATIONS_KEY } from "./constants";

export function pluginUiTranslationsQueryOptions(
  plugin: string,
  version: string,
  locale: string
) {
  const base = getPluginUiAssetBase({ plugin, version });
  return {
    queryKey: [PLUGIN_UI_TRANSLATIONS_KEY, plugin, version, locale],
    queryFn: ({ signal }: { signal?: AbortSignal }) =>
      getPluginUiTranslations(base, locale, signal),
    staleTime: Infinity,
    retry: false,
  };
}
