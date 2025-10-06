import { useQuery } from "react-query";
import { createContext } from "react";

import { defaultQueryConfig } from "shared/api";

import { configSync, getConfig } from "./api";

export const useConfigQuery = () => {
  return useQuery({
    ...defaultQueryConfig,
    queryKey: getConfig.name,
    queryFn: getConfig,
    initialData: configSync,
  });
};

export const ConfigQueryContext = createContext<ReturnType<
  typeof useConfigQuery
> | null>(null);
