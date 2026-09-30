import { create } from "axios";

import { getValidSessionToken } from "shared/entity/session/getValidSessionToken";

import { getPluginUiPages } from "./api";
import { PLUGIN_UI_PAGES_KEY, REFETCH_INTERVAL } from "./constants";

export function pluginUiPagesQueryOptions(
  scope: number,
  isAuthEnabled: boolean
) {
  return {
    queryKey: [PLUGIN_UI_PAGES_KEY, isAuthEnabled, scope] as const,
    queryFn: async ({ signal }: { signal?: AbortSignal }) => {
      const token = isAuthEnabled
        ? await getValidSessionToken(scope, signal)
        : "";
      const axios = create({
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      return getPluginUiPages(axios, signal);
    },
    refetchInterval: REFETCH_INTERVAL,
    keepPreviousData: false,
    cacheTime: 0,
  };
}
