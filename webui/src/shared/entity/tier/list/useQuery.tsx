import { useQuery } from "react-query";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { TIERS_LIST_KEY } from "./constants";
import { getTiers } from "./api";

export const useTiers = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery({
    queryKey: [TIERS_LIST_KEY],
    queryFn: () => getTiers(axios),
    ...queryConfig,
  });
};
