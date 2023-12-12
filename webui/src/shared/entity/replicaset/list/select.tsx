import { InstanceType } from "../../instance";
import { ReplicasetType } from "../common/types";
import { mapReplicasetToClient } from "../common";

import { ServerReplicasetsListType } from "./types";

export const select = (
  data: ServerReplicasetsListType
): { replicasets: ReplicasetType[]; instances: InstanceType[] } => {
  const replicasets = data.map(mapReplicasetToClient);

  return {
    replicasets,
    // todo нужен уникальный id для instances
    instances: replicasets.map((replicaset) => replicaset.instances).flat(1),
  };
};
