import { Override } from "../../../utils/tsUtils";
import {
  ServerInstanceType,
  InstanceType,
  InstanceNodeType,
} from "../../instance/common/types";
import { NodeType } from "../../tier";

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
export type ReplicasetNodeType = ReplicasetType & {
  instances: InstanceNodeType[];
  type: NodeType.Replicaset;
  open: boolean;
  syntheticId: string;
};
