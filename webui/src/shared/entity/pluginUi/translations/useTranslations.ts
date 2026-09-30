import { useQuery } from "react-query";

import { DEFAULT_LOCALE } from "shared/intl/constants";
import { useTranslation } from "shared/intl";

import { pluginUiTranslationsQueryOptions } from "./queryOptions";

export function usePluginUiTranslations(
  plugin: string,
  version: string,
  titles: string[]
) {
  const { locale } = useTranslation();
  const options = (language: string) =>
    pluginUiTranslationsQueryOptions(plugin, version, language);
  const current = useQuery(options(locale));
  const fallback = useQuery({
    ...options(DEFAULT_LOCALE),
    enabled:
      locale !== DEFAULT_LOCALE &&
      (current.isError ||
        (current.isSuccess && titles.some((title) => !current.data?.[title]))),
  });
  return (title: string) =>
    current.data?.[title] || fallback.data?.[title] || title;
}
