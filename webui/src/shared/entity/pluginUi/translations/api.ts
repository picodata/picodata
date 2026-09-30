import axios from "axios";

import { Dictionary } from "./types";

export async function getPluginUiTranslations(
  base: string,
  locale: string,
  signal?: AbortSignal
): Promise<Dictionary> {
  const { data } = await axios.get<unknown>(
    `${base}/i18n/${encodeURIComponent(locale)}.json`,
    { signal }
  );
  if (!data || typeof data !== "object" || Array.isArray(data)) return {};
  return Object.fromEntries(
    Object.entries(data).filter(
      ([, value]) => typeof value === "string" && value.trim()
    )
  ) as Dictionary;
}
