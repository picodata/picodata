import { mapInstanceToClient } from "../../instance";

import { ServerReplicasetType } from "./types";

export const mapReplicasetToClient = <
  T extends Pick<ServerReplicasetType, "instances">
>(
  replicaset: T
) => {
  return {
    ...replicaset,
    instances: replicaset.instances.map(mapInstanceToClient),
  };
};
