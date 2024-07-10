import axios from "axios";

import { ServerReplicasetsListType } from "./types";
import { GET_REPLICASETS_URL } from "./constants";

export const getReplicasets = async () => {
  const response = await axios.get<ServerReplicasetsListType>(
    GET_REPLICASETS_URL
  );

  return response.data;
};
