import { AxiosInstance } from "axios";

import { GET_MEMORY_URL } from "./constants";
import { ServerMemoryType } from "./common";

export const getMemory = async (axios: AxiosInstance) => {
  const response = await axios.get<ServerMemoryType>(GET_MEMORY_URL);
  return response.data;
};
