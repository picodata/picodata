import { useMemo } from "react";

import { useUSP } from "shared/filters/hooks/useUSP";
import { ClientInstanceType } from "store/slices/types";

import { filterByValue } from "./config";

export const useFilterBy = () => {
  return useUSP({
    key: "filterBy",
    schema: filterByValue,
  });
};

export const useInstancesFiltersData = (instances: ClientInstanceType[]) => {
  return useMemo(() => {
    const domainMap = new Map<string, { key: string; value: string }>();

    instances.forEach((instance) => {
      instance.failureDomain.forEach(({ key, value }) => {
        domainMap.set(`${key}:${value}`, { key, value });
      });
    });

    return {
      domains: [...domainMap.values()],
    };
  }, [instances]);
};
