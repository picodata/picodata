import axios from "axios";

import { User } from "../types/types";

import { GET_USERS_URL } from "./constants";

export const getUsersInfo = async () => {
  const response = await axios.get<User[]>(GET_USERS_URL);

  return response.data;
};
