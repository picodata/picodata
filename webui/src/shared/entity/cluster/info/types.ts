export type ServerClusterInfoType = {
  clusterName: string;
  clusterVame: string;
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
  plugins: string[];
  currentInstaceVersion?: string;
};
