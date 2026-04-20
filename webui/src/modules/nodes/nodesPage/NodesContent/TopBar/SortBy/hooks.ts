import { useUSP } from "shared/router/hooks/useUSP";

import { sortValue } from "./config";

export const useSortBy = () => {
  return useUSP({
    key: "sortBy",
    schema: sortValue,
  });
};
