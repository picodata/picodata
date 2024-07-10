import { useQuery } from "react-query";

import { useQueryConfig } from "shared/api";

import { Role } from "../types/types";

import { ROLES_INFO_KEY } from "./constants";
import { getRolesInfo } from "./getRolesInfo";

export const useRolesInfoQuery = () => {
  const queryConfig = useQueryConfig();

  return useQuery<Role[]>(ROLES_INFO_KEY, getRolesInfo, {
    ...queryConfig,
  });
};
