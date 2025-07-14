import { createContext, useContext } from "react";

import { retryDelay, retry } from "../utils/retry";

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

export const useQueryConfig = () => {
  const refetchContext = useContext(RefetchContext);

  return {
    refetchOnMount: false,
    refetchOnReconnect: true,
    networkMode: "online" as const,
    retryDelay,
    retry,

    refetchOnWindowFocus: true,
    refetchInterval() {
      return refetchContext.interval();
    },
  };
};
