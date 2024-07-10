import { useMemo } from "react";

import { useUSP } from "shared/router/hooks/useUSP";
import { InstanceType } from "shared/entity/instance";

import { filterByValue } from "./config";

export const useFilterBy = () => {
  return useUSP({
    key: "filterBy",
    schema: filterByValue,
  });
};

export const useInstancesFiltersData = (instances: InstanceType[]) => {
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
