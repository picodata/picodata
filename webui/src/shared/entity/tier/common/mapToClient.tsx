import { mapReplicasetToClient } from "../../replicaset";

import { ServerTierType } from "./types";

export const mapTierToClient = <T extends Pick<ServerTierType, "replicasets">>(
  tier: T
) => {
  return {
    ...tier,
    replicasets: tier.replicasets.map(mapReplicasetToClient),
  };
};
