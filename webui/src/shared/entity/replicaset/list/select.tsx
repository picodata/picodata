import { InstanceType } from "../../instance";
import { ReplicasetType } from "../common/types";
import { mapReplicasetToClient } from "../common";

import { ServerReplicasetsListType } from "./types";

export interface SelectedList {
  replicasets: ReplicasetType[];
  instances: InstanceType[];
}

export const select = (data: ServerReplicasetsListType): SelectedList => {
  const replicasets = data.map(mapReplicasetToClient);

  return {
    replicasets,
    // todo нужен уникальный id для instances
    instances: replicasets.map((replicaset) => replicaset.instances).flat(1),
  };
};

export type { ServerReplicasetsListType };
