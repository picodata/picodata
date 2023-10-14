// eslint-disable-next-line no-restricted-imports
import { useSelectFilter } from "../../../../shared/filters/hooks/useSelectFilter";
import { groupByValue } from "./config";

export const useGroupByFilter = () => {
  return useSelectFilter({
    key: "groupBy",
    schema: groupByValue,
    defaultValue: "REPLICASETS",
  });
};
