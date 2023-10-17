import { useMemo } from "react";
import { TSortValue } from "./Filters/SortBy/config";
import { sortByString } from "components/shared/utils/string/sort";
import { formatFailDomain } from "./utils";
import { ClientInstanceType } from "store/slices/types";

export const useSortedInstances = (
  instances: ClientInstanceType[],
  sortBy?: TSortValue
) => {
  return useMemo(() => {
    if (!sortBy) return instances;

    return [...instances].sort((a, b) => {
      if (sortBy.by === "FAILURE_DOMAIN") {
        return sortByString(
          formatFailDomain(a.failureDomain),
          formatFailDomain(b.failureDomain),
          {
            order: sortBy.order,
          }
        );
      }

      return sortByString(a.name, b.name, { order: sortBy.order });
    });
  }, [instances, sortBy]);
};
