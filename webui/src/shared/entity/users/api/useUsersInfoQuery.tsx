import { useQuery } from "react-query";

import { useAuthAxios, useQueryConfig } from "shared/api";

import { User } from "../types/types";

import { USERS_INFO_KEY } from "./constants";
import { getUsersInfo } from "./getUsersInfo";

export const useUsersInfoQuery = () => {
  const queryConfig = useQueryConfig();
  const axios = useAuthAxios();

  return useQuery<User[]>({
    queryKey: [USERS_INFO_KEY],
    queryFn: () => getUsersInfo(axios),
    ...queryConfig,
  });
};
