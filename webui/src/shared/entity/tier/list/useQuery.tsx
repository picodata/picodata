import { useQuery } from "react-query";

import { useQueryConfig } from "shared/api";

import { TIERS_LIST_KEY } from "./constants";
import { getTiers } from "./api";
import { select } from "./select";

export const useTiers = () => {
  const queryConfig = useQueryConfig();

  return useQuery(TIERS_LIST_KEY, getTiers, {
    ...queryConfig,
    select,
  });
};
