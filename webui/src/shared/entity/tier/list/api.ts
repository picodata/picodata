import { AxiosInstance } from "axios";

import { ServerTiersListType } from "./types";
import { GET_TIERS_URL } from "./constants";

export const getTiers = async (axios: AxiosInstance) => {
  const response = await axios.get<ServerTiersListType>(GET_TIERS_URL);
  return response.data;
};
