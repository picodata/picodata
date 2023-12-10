import { useQuery } from "react-query";

import { useQueryConfig } from "shared/api";

import { ClusterInfoType } from "./types";
import { CLUSTER_INFO_KEY } from "./constants";
import { getClusterInfo } from "./api";

export const useClusterInfo = () => {
  const queryConfig = useQueryConfig();

  return useQuery<ClusterInfoType>(CLUSTER_INFO_KEY, getClusterInfo, {
    ...queryConfig,
  });
};
