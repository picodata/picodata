import { useQuery } from "react-query";
import { useMemo } from "react";

import { useQueryConfig } from "shared/api";
import { useAppConfig } from "shared/entity/config";
import { useSessionScope } from "shared/session";

import { pluginUiPagesQueryOptions } from "./queryOptions";

export const usePluginUiPages = () => {
  const queryConfig = useQueryConfig();
  const scope = useSessionScope();
  const { isAuthEnabled } = useAppConfig();
  const options = useMemo(
    () => pluginUiPagesQueryOptions(scope, isAuthEnabled),
    [scope, isAuthEnabled]
  );

  return useQuery({
    ...queryConfig,
    ...options,
  });
};
