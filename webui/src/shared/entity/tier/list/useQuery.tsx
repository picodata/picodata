import { useQuery } from "react-query";

import { useSessionScope } from "shared/session";
import { useAuthAxios, useQueryConfig } from "shared/api";

import { TIERS_LIST_KEY } from "./constants";
import { getTiers } from "./api";

export const useTiers = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();
  const scope = useSessionScope();

  return useQuery({
    queryKey: [TIERS_LIST_KEY, scope],
    queryFn: () => getTiers(axios),
    ...queryConfig,
  });
};
