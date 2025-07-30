export type ServerClusterInfoType = {
  clusterName: string;
  capacityUsage: number;
  systemCapacityUsage: number;
  memory: {
    used: number;
    usable: number;
  };
  systemMemory: {
    used: number;
    usable: number;
  };
  replicasetsCount: number;
  instancesCurrentStateOnline: number;
  instancesCurrentStateOffline: number;
  currentInstaceVersion: string;
  plugins: string[];
};

export type ClusterInfoType = ServerClusterInfoType;
