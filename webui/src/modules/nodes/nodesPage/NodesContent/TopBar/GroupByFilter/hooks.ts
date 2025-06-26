// eslint-disable-next-line no-restricted-imports
import { useUSP } from "shared/router/hooks/useUSP";

import { groupByValue } from "./config";

export const useGroupByFilter = () => {
  return useUSP({
    key: "groupBy",
    schema: groupByValue,
    defaultValue: "TIERS",
  });
};
