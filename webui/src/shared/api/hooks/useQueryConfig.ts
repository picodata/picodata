import { createContext, useContext } from "react";
import { UseQueryOptions } from "react-query";
import { AxiosError } from "axios";

import { isAuthError } from "shared/entity/session";
import { ConfigQueryContext } from "shared/entity/config";

import { retryDelay, retry } from "../utils/retry";
import { defaultQueryConfig } from "../client";

export const defaultRefetchInterval = 10 * 1000;

export interface RefetchContextOptions {
  interval: () => number;
}

/**
 * Provide this context to a component that uses `useQuery` (`get*Info`)
 * to set/update query refetch interval
 */
export const RefetchContext = createContext<RefetchContextOptions>({
  interval: () => defaultRefetchInterval,
});

export const useQueryConfig = <T>() => {
  const refetchContext = useContext(RefetchContext);
  const config = useContext(ConfigQueryContext);

  return {
    ...defaultQueryConfig,
    // networkMode: "online" as const,
    retryDelay,
    retry,
    refetchInterval() {
      return refetchContext.interval();
    },
    onError(err) {
      if (isAuthError(err)) {
        config?.refetch();
      }
    },
  } satisfies UseQueryOptions<T, AxiosError>;
};
