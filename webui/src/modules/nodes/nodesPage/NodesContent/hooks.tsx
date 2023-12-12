import { useMemo } from "react";

import { sortByString } from "shared/utils/string/sort";
import { InstanceType } from "shared/entity/instance";

import { TSortValue } from "./TopBar/SortBy/config";
import { formatFailDomains } from "./utils";
import { TFilterByValue } from "./TopBar/FilterBy/config";

export const useFilteredInstances = (
  instances?: InstanceType[],
  filteredBy?: TFilterByValue
) => {
  return useMemo(() => {
    if (!instances) return [];

    if (!filteredBy) return instances;

    let filteredInstances = instances;

    if (filteredBy.domain !== undefined) {
      filteredInstances = instances.filter((instance) => {
        return filteredBy.domain?.every((domainFilter) => {
          return instance.failureDomain.find(
            (domain) =>
              domain.key === domainFilter.key &&
              domainFilter.value.includes(domain.value)
          );
        });
      });
    }

    return filteredInstances;
  }, [instances, filteredBy]);
};

export const useSortedInstances = (
  instances?: InstanceType[],
  sortBy?: TSortValue
) => {
  return useMemo(() => {
    if (!instances) return [];

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
