import { useQuery } from "react-query";

import { useSessionScope } from "shared/session";
import { useAuthAxios } from "shared/api";

import { getFullInstance } from "./api";
import { INSTANCE_KEY } from "./constants";

export const useFullInstance = (id: string) => {
  const axios = useAuthAxios();
  const scope = useSessionScope();

  return useQuery({
    queryKey: [INSTANCE_KEY, id, scope],
    queryFn: () => getFullInstance(axios, id),
    cacheTime: 0,
    staleTime: 0,
    refetchOnMount: "always",
    refetchOnWindowFocus: false,
    refetchOnReconnect: true,

    retry: false,
  });
};
