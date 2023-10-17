import { useMemo } from "react";
import { ClientInstanceType } from "store/slices/types";

// export const useSortBy = () => {
//   return useUSP({
//     key: "sortBy",
//     schema: sortByValue,
//   });
// };

export const useInstancesFiltersData = (instances: ClientInstanceType[]) => {
  return useMemo(() => {
    const names: string[] = [];

    instances.forEach((instance) => {
      names.push(instance.name);
    });

    return { instancesNames: [...new Set(names)] };
  }, [instances]);
};
