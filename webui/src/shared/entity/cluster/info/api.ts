// import axios from "axios";

// import { ServerClusterInfoType } from "./types";
// import { GET_CLUSTER_URL } from "./constants";

export const getClusterInfo = async () => {
  return {
    capacityUsage: 80,
    memory: {
      used: 50,
      usable: 80,
    },
    replicasetsCount: 4,
    instancesCurrentGradeOnline: 3,
    instancesCurrentGradeOffline: 2,
    currentInstaceVersion: "123",
  };
  // const response = await axios.get<ServerClusterInfoType>(GET_CLUSTER_URL);

  // return response.data;
};
