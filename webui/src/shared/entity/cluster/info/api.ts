import axios from "axios";

import { ServerClusterInfoType } from "./types";
import { GET_CLUSTER_URL } from "./constants";

export const getClusterInfo = async () => {
  const response = await axios.get<ServerClusterInfoType>(GET_CLUSTER_URL);

  return response.data;
};
