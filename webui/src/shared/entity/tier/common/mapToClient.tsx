import { mapReplicasetToClient } from "../../replicaset";

import { ServerTierType } from "./types";

export const mapTierToClient = <T extends Pick<ServerTierType, "replicasets">>(
  tier: T
) => {
  const replicasets = tier.replicasets.map(mapReplicasetToClient);
  return {
    ...tier,
    replicasets,
    hasRaftLeader: replicasets.some(({ hasRaftLeader }) => hasRaftLeader),
  };
};
