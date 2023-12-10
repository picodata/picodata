import { Override } from "../../utils/tsUtils";
import { ServerInstanceType, InstanceType } from "../instance/types";

export type ServerReplicasetType = {
  id: string;
  instanceCount: number;
  instances: ServerInstanceType[];
  version: string;
  grade: string;
  capacityUsage: number;
  memory: {
    usable: number;
    used: number;
  };
};

export type ReplicasetType = Override<
  ServerReplicasetType,
  { instances: InstanceType[] }
>;
