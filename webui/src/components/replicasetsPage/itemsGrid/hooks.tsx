import { useMemo } from "react";
import { TSortValue } from "./TopBar/SortBy/config";
import { sortByString } from "components/shared/utils/string/sort";
import { formatFailDomains } from "./utils";
import { ClientInstanceType } from "store/slices/types";
import { TFilterByValue } from "./TopBar/FilterBy/config";

export const useFilteredInstances = (
  instances: ClientInstanceType[],
  filteredBy?: TFilterByValue
) => {
  return useMemo(() => {
    if (!filteredBy) return instances;

    let filteredInstances = instances;

    if (filteredBy.domain !== undefined) {
      filteredInstances = instances.filter((instance) => {
        return filteredBy.domain?.every((domainFilter) => {
          return instance.failureDomain.find(
            (domain) =>
              domain.key === domainFilter.key &&
              domain.value === domainFilter.value
          );
        });
      });
    }

    return filteredInstances;
  }, [instances, filteredBy]);
};

export const useSortedInstances = (
  instances: ClientInstanceType[],
  sortBy?: TSortValue
) => {
  return useMemo(() => {
    if (!sortBy) return instances;

    return [...instances].sort((a, b) => {
      if (sortBy.by === "FAILURE_DOMAIN") {
        return sortByString(
          formatFailDomains(a.failureDomain),
          formatFailDomains(b.failureDomain),
          {
            order: sortBy.order,
          }
        );
      }

      return sortByString(a.name, b.name, { order: sortBy.order });
    });
  }, [instances, sortBy]);
};
