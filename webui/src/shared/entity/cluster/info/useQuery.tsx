import { useQuery } from "react-query";
import { AxiosError } from "axios";

import { useSessionScope } from "shared/session";
import { useAuthAxios, useQueryConfig } from "shared/api";

import { ServerClusterInfoType } from "./types";
import { CLUSTER_INFO_KEY } from "./constants";
import { getClusterInfo } from "./api";

export const useClusterInfo = (enabled = true) => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();
  const scope = useSessionScope();

  return useQuery<ServerClusterInfoType, AxiosError>({
    queryKey: [CLUSTER_INFO_KEY, scope],
    queryFn: () => getClusterInfo(axios),
    ...queryConfig,
    enabled,
  });
};
