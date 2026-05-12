import { useQuery } from "react-query";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { getMemory } from "./api";
import { MEMORY_KEY, REFETCH_INTERVAL } from "./constants";

export const useMemory = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery({
    queryKey: [MEMORY_KEY],
    queryFn: () => getMemory(axios),
    ...queryConfig,
    refetchInterval: REFETCH_INTERVAL,
  });
};
