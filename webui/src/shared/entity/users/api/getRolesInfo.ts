import axios from "axios";

import { Role } from "../types/types";

import { GET_ROLES_URL } from "./constants";

export const getRolesInfo = async () => {
  const response = await axios.get<Role[]>(GET_ROLES_URL);

  return response.data;
};
