import { Override } from "../../../utils/tsUtils";
import {
  ServerInstanceType,
  InstanceType,
  InstanceNodeType,
} from "../../instance/common/types";
import { NodeType } from "../../tier";
import { Memory } from "../../memory";

export type ServerReplicasetType = {
  name: string;
  instanceCount: number;
  instances: ServerInstanceType[];
  version: string;
  state: "Online" | "Offline" | "Expelled";
  capacityUsage: number;
  replicasetState: "ready" | "not-ready";
};

export type ReplicasetType = Override<
  ServerReplicasetType,
  { instances: InstanceType[] }
> &
  Memory & {
    currentInstanceCount: number;
    hasRaftLeader: boolean;
    hasVoter: boolean;
  };
export type ReplicasetNodeType = ReplicasetType & {
  instances: InstanceNodeType[];
  type: NodeType.Replicaset;
  open: boolean;
  syntheticId: string;
};
