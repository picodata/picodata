import { Override } from "../../../utils/tsUtils";
import { ServerInstanceType, InstanceType } from "../../instance/common/types";

export type ServerReplicasetType = {
  id: string;
  instanceCount: number;
  instances: ServerInstanceType[];
  version: string;
  grade: "Online" | "Offline";
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