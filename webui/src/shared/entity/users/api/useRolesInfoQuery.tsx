import { useQuery } from "react-query";

import { useSessionScope } from "shared/session";
import { useAuthAxios, useQueryConfig } from "shared/api";

import { ROLES_INFO_KEY } from "./constants";
import { getRolesInfo } from "./getRolesInfo";

export const useRolesInfoQuery = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();
  const scope = useSessionScope();

  return useQuery({
    queryKey: [ROLES_INFO_KEY, scope],
    queryFn: () => getRolesInfo(axios),
    ...queryConfig,
  });
};
