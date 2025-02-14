import { Override } from "../../../utils/tsUtils";
import { ServerInstanceType, InstanceType } from "../../instance/common/types";

export type ServerReplicasetType = {
  name: string;
  instanceCount: number;
  instances: ServerInstanceType[];
  version: string;
  state: "Online" | "Offline" | "Expelled";
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
