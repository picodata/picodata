import { useMemo } from "react";

import { TDomain, TKeyValueFilter } from "./types";

export const useKeysValuesData = (
  domains: TDomain[],
  filter: TKeyValueFilter
) => {
  return useMemo(() => {
    const keys = [...new Set(domains.map((domain) => domain.key))];

    if (filter.key) {
      return {
        keys,
        values: domains
          .filter((domain) => domain.key === filter.key)
          .map((domain) => domain.value),
      };
    }

    return { keys, values: domains.map((domain) => domain.value) };
  }, [domains, filter.key]);
};
