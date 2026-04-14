import { mapInstanceToClient } from "../../instance";

import { ServerReplicasetType } from "./types";

export const mapReplicasetToClient = <
  T extends Pick<ServerReplicasetType, "instances">
>(
  replicaset: T
) => {
  return {
    ...replicaset,
    currentInstanceCount:
      replicaset.instances.filter(
        ({ currentState }) => currentState === "Online"
      )?.length || 0,
    instances: replicaset.instances.map(mapInstanceToClient),
  };
};
