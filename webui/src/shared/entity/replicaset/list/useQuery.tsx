import { useQuery } from "react-query";
import { AxiosError } from "axios";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { REPLICASETS_LIST_KEY } from "./constants";
import { getReplicasets } from "./api";
import { select, SelectedList, ServerReplicasetsListType } from "./select";

export const useReplicasets = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery<ServerReplicasetsListType, AxiosError, SelectedList>({
    queryKey: [REPLICASETS_LIST_KEY],
    queryFn: () => getReplicasets(axios),
    ...queryConfig,
    select,
  });
};
