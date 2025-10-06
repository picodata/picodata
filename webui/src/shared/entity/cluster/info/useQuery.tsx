import { useQuery } from "react-query";
import { AxiosError } from "axios";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { ClusterInfoType } from "./types";
import { CLUSTER_INFO_KEY } from "./constants";
import { getClusterInfo } from "./api";

export const useClusterInfo = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery<ClusterInfoType, AxiosError>({
    queryKey: [CLUSTER_INFO_KEY],
    queryFn: () => getClusterInfo(axios),
    ...queryConfig,
  });
};
