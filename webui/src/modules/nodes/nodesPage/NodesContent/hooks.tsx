import { useMemo } from "react";

import { sortByString, SortByStringOptions } from "shared/utils/string/sort";
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

    return sortByStringProp(
      instances,
      (a) =>
        sortBy.by === "FAILURE_DOMAIN"
          ? // Fallback to sorting by-name if failure domain is empty
            // Make sure to make empty instances "float to the top" by prefixing with `_`
            formatFailDomains(a.failureDomain) || `_${a.name}`
          : a.name,
      { order: sortBy.order }
    );
  }, [instances, sortBy]);
};

export function useSortedByString<T>(
  array: T[] | undefined,
  prop: (x: T) => string,
  options?: SortByStringOptions
) {
  if (!array) return [];

  return sortByStringProp<T>(array, prop, options);
}

export function sortByStringProp<T>(
  array: T[],
  prop: (x: T) => string,
  options?: SortByStringOptions
) {
  return [...array].sort((a, b) => sortByString(prop(a), prop(b), options));
}
