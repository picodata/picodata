import { useQuery } from "react-query";

import { useQueryConfig } from "shared/api";

import { REPLICASETS_LIST_KEY } from "./constants";
import { getReplicasets } from "./api";
import { select } from "./select";

export const useReplicasets = () => {
  const queryConfig = useQueryConfig();

  return useQuery(REPLICASETS_LIST_KEY, getReplicasets, {
    ...queryConfig,
    select,
  });
};
