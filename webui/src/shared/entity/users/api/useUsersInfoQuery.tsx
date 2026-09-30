import { useQuery } from "react-query";

import { useSessionScope } from "shared/session";
import { useAuthAxios, useQueryConfig } from "shared/api";

import { USERS_INFO_KEY } from "./constants";
import { getUsersInfo } from "./getUsersInfo";

export const useUsersInfoQuery = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();
  const scope = useSessionScope();

  return useQuery({
    queryKey: [USERS_INFO_KEY, scope],
    queryFn: () => getUsersInfo(axios),
    ...queryConfig,
  });
};
