import { Override } from "../../../utils/tsUtils";
import {
  ReplicasetNodeType,
  ReplicasetType,
  ServerReplicasetType,
} from "../../replicaset/common/types";
import { Memory } from "../../memory";

export type ServerTierType = {
  name: string;
  services: string[];
  replicasetCount: number;
  instanceCount: number;
  rf: number;
  bucketCount: number;
  can_vote: boolean;
  replicasets: ServerReplicasetType[];
  capacityUsage?: number;
};

export enum NodeType {
  Tier = "Tier",
  Replicaset = "Replicaset",
  Instance = "Instance",
}

export type TierType = Override<
  ServerTierType,
  { replicasets: ReplicasetType[] }
> &
  Memory & {
    hasRaftLeader: boolean;
  };
export type TierNodeType = TierType & {
  open: boolean;
  syntheticId: string;
  type: NodeType.Tier;
  replicasets: ReplicasetNodeType[];
};
