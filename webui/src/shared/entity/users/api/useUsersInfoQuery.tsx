import { useQuery } from "react-query";

import { useQueryConfig } from "shared/api";

import { User } from "../types/types";

import { USERS_INFO_KEY } from "./constants";
import { getUsersInfo } from "./getUsersInfo";

export const useUsersInfoQuery = () => {
  const queryConfig = useQueryConfig();

  return useQuery<User[]>(USERS_INFO_KEY, getUsersInfo, {
    ...queryConfig,
  });
};
