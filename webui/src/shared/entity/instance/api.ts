import { AxiosInstance } from "axios";

import { GET_INSTANCE_URL } from "./constants";
import { FullInstance } from "./common";

export const getFullInstance = async (axios: AxiosInstance, id: string) => {
  const response = await axios.get<FullInstance>(`${GET_INSTANCE_URL}/${id}`);
  return response.data;
};
