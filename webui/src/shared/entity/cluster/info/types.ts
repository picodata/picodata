export type ServerClusterInfoType = {
  capacityUsage: number;
  memory: {
    used: number;
    usable: number;
  };
  replicasetsCount: number;
  instancesCurrentGradeOnline: number;
  instancesCurrentGradeOffline: number;
  currentInstaceVersion: string;
};

export type ClusterInfoType = ServerClusterInfoType;
