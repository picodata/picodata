import { TierNodeType, TierType } from "../common";
import { sortReplicaset } from "../../replicaset";
import { sortInstances } from "../../instance";

export const sortTiers = <T extends TierType | TierNodeType>(
  tiers: T[]
): T[] => {
  let resultTiers = [...tiers];
  resultTiers.sort((tierA, tierB) => (tierA.name < tierB.name ? -1 : 1));
  resultTiers = resultTiers.map((tier) => ({
    ...tier,
    replicasets: sortReplicaset(tier.replicasets).map((replicaset) => ({
      ...replicaset,
      instances: sortInstances(replicaset.instances),
    })),
  }));
  return resultTiers;
};
