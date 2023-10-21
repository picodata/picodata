// eslint-disable-next-line no-restricted-imports
import { useUSP } from "../../../../../../shared/filters/hooks/useUSP";

import { sortValue } from "./config";

export const useSortBy = () => {
  return useUSP({
    key: "sortBy",
    schema: sortValue,
  });
};
