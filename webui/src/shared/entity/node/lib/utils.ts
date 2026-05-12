import { ServerTierType, TierType } from "../../tier";
import { ReplicasetType } from "../../replicaset";
import { InstanceType } from "../../instance";
import { TierMemory } from "../../memory";

export const getNodes = (
  initialTiers: ServerTierType[] | undefined = [],
  memory: TierMemory[] | undefined = []
) => {
  const replicasets: ReplicasetType[] = [];
  const instances: InstanceType[] = [];
  const tiers: TierType[] = initialTiers.map((_tier) => {
    let tierHasRaftLeader = false;
    const currentMemoryTier = memory.find(
      ({ name: memoryTierName }) => memoryTierName === _tier.name
    );

    const currentReplicasets = _tier.replicasets.map((_replicaset) => {
      let replicasetHasVoter = false;
      let replicasetHasRaftLeader = false;

      const currentInstances: InstanceType[] = _replicaset.instances.map(
        (_instance) => {
          if (_instance.isVoter) {
            replicasetHasVoter = true;
          }
          if (_instance.isRaftLeader) {
            replicasetHasRaftLeader = true;
            tierHasRaftLeader = true;
          }
          return {
            ..._instance,
            failureDomain: Object.entries(_instance.failureDomain).map(
              ([key, value]) => ({
                key,
                value,
              })
            ),
          };
        }
      );
      instances.push(...currentInstances);

      const currentMemoryReplicaset = (
        currentMemoryTier?.replicasets || []
      ).find(
        ({ name: memoryReplicasetName }) =>
          memoryReplicasetName === _replicaset.name
      );
      return {
        ..._replicaset,
        hasRaftLeader: replicasetHasRaftLeader,
        hasVoter: replicasetHasVoter,
        instances: currentInstances,
        currentInstanceCount:
          currentInstances.filter(
            ({ currentState }) => currentState === "Online"
          )?.length || 0,
        usable: currentMemoryReplicaset?.usable ?? 0,
        used: currentMemoryReplicaset?.used ?? 0,
      };
    });
    replicasets.push(...currentReplicasets);

    return {
      ..._tier,
      hasRaftLeader: tierHasRaftLeader,
      replicasets: currentReplicasets,
      usable: currentMemoryTier?.usable ?? 0,
      used: currentMemoryTier?.used ?? 0,
    };
  });

  return {
    tiers,
    replicasets,
    instances,
  };
};
