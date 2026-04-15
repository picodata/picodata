import { mapInstanceToClient } from "../../instance";

import { ServerReplicasetType } from "./types";

export const mapReplicasetToClient = <
  T extends Pick<ServerReplicasetType, "instances">
>(
  replicaset: T
) => {
  let hasRaftLeader = false;
  let hasVoter = false;
  replicaset.instances.forEach(({ isRaftLeader, isVoter }) => {
    if (isRaftLeader) {
      hasRaftLeader = true;
    }
    if (isVoter) {
      hasVoter = true;
    }
  });
  return {
    ...replicaset,
    currentInstanceCount:
      replicaset.instances.filter(
        ({ currentState }) => currentState === "Online"
      )?.length || 0,
    instances: replicaset.instances.map(mapInstanceToClient),
    hasRaftLeader,
    hasVoter,
  };
};
