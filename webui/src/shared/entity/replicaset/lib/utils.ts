import { ReplicasetNodeType, ReplicasetType } from "../common";

export const sortReplicaset = <T extends ReplicasetType | ReplicasetNodeType>(
  replicasets: T[]
): T[] => {
  const resultReplicasets = [...replicasets];
  resultReplicasets.sort((replicasetA, replicasetB) =>
    replicasetA.name < replicasetB.name ? -1 : 1
  );
  return resultReplicasets;
};
