export type ServerClusterInfoType = {
  capacityUsage: number;
  memory: {
    used: number;
    usable: number;
  };
  replicasetsCount: number;
  instancesCurrentStateOnline: number;
  instancesCurrentStateOffline: number;
  currentInstaceVersion: string;
};

export type ClusterInfoType = ServerClusterInfoType;
