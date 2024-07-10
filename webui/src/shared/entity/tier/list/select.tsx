import { InstanceType } from "../../instance";
import { ReplicasetType } from "../../replicaset";
import { mapTierToClient } from "../common";
import { TierType } from "../common/types";

import { ServerTiersListType } from "./types";

export const select = (
  data: ServerTiersListType
): {
  tiers: TierType[];
  replicasets: ReplicasetType[];
  instances: InstanceType[];
} => {
  const tiers = data.map(mapTierToClient);
  const replicasets = tiers.map((tier) => tier.replicasets).flat();

  return {
    tiers,
    replicasets,
    instances: replicasets.map((replicaset) => replicaset.instances).flat(1),
  };
};
