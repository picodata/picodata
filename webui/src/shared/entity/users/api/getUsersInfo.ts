import { AxiosInstance } from "axios";

import { User } from "../types/types";

import { GET_USERS_URL } from "./constants";

export const getUsersInfo = async (axios: AxiosInstance) => {
  const response = await axios.get<User[]>(GET_USERS_URL);

  return response.data;
};
