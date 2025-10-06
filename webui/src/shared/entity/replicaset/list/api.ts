import { AxiosInstance } from "axios";

import { ServerReplicasetsListType } from "./types";
import { GET_REPLICASETS_URL } from "./constants";

export const getReplicasets = async (axios: AxiosInstance) => {
  const response = await axios.get<ServerReplicasetsListType>(
    GET_REPLICASETS_URL
  );

  return response.data;
};
