import { useQuery } from "react-query";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { ROLES_INFO_KEY } from "./constants";
import { getRolesInfo } from "./getRolesInfo";

export const useRolesInfoQuery = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery({
    queryKey: [ROLES_INFO_KEY],
    queryFn: () => getRolesInfo(axios),
    ...queryConfig,
  });
};
