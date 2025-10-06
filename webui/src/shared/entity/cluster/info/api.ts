import { AxiosInstance } from "axios";

import { ServerClusterInfoType } from "./types";
import { GET_CLUSTER_URL } from "./constants";

export const getClusterInfo = async (axios: AxiosInstance) => {
  const response = await axios.get<ServerClusterInfoType>(GET_CLUSTER_URL);

  return response.data;
};
