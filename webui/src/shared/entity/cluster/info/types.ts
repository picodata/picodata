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
  plugins: string[];
};

export type ClusterInfoType = ServerClusterInfoType;
