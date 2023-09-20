export interface InstanceType {
  name: string;
  targetGrade: string;
  currentGrade: string;
  failureDomain: string;
  version: string;
  isLeader: boolean;
}
export interface ReplicasetType {
  id: string;
  instanceCount: number;
  instances: InstanceType[];
  version: string;
  grade: string;
  capacity: string;
}

export interface ClusterInfoType {
  capacityUsage: string;
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
