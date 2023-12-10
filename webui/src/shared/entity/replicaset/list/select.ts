import { InstanceType } from "../../instance";
import { ReplicasetType } from "../types";

import { ServerReplicasetsListType } from "./types";

export const select = (
  data: ServerReplicasetsListType
): { replicasets: ReplicasetType[]; instances: InstanceType[] } => {
  const replicasets = data.map((replicaset) => {
    return {
      ...replicaset,
      instances: replicaset.instances.map((instance) => ({
        ...instance,
        failureDomain: Object.entries(instance.failureDomain).map(
          ([key, value]) => ({
            key,
            value,
          })
        ),
      })),
    };
  });

  return {
    replicasets,
    // todo нужен уникальный id для instances
    instances: replicasets.map((replicaset) => replicaset.instances).flat(1),
  };
};
