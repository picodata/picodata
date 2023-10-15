import { useMemo } from "react";
import { TSortByValue } from "./Filters/SortBy/config";
import { InstanceType } from "store/slices/types";
import { sortByString } from "components/shared/utils/string/sort";

export const useSortedInstances = (
  instances: InstanceType[],
  sortBy?: TSortByValue
) => {
  return useMemo(() => {
    return [...instances].sort((a, b) => {
      if (sortBy === "FAILURE_DOMAIN") {
        return sortByString(a.failureDomain, b.failureDomain);
      }

      return sortByString(a.name, b.name);
    });
  }, [instances, sortBy]);
};
