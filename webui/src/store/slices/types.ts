export interface InstanceType {
  name: string;
  targetGrade: string;
  currentGrade: string;
  failureDomain: Record<string, string>;
  version: string;
  isLeader: boolean;
}
export interface ReplicasetType {
  id: string;
  instanceCount: number;
  instances: InstanceType[];
  version: string;
  grade: string;
  capacityUsage: number;
  memory: {
    usable: number;
    used: number;
  };
}

export interface ClusterInfoType {
  capacityUsage: number;
  memory: {
    used: number;
    usable: number;
  };
  replicasetsCount: number;
  instancesCurrentGradeOnline: number;
  instancesCurrentGradeOffline: number;
  currentInstaceVersion: string;
}

export enum ActionTypes {
  getClusterInfoType = "cluster/getClusterInfo",
  getReplicasetsType = "cluster/getReplicasets",
}

export type ClientInstanceType = Omit<InstanceType, "failureDomain"> & {
  failureDomain: Array<{ key: string; value: string }>;
};

export type ClientReplicasetType = Omit<ReplicasetType, "instances"> & {
  instances: ClientInstanceType[];
};
