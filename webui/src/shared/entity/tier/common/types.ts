import { Override } from "../../../utils/tsUtils";
import {
  ReplicasetType,
  ServerReplicasetType,
} from "../../replicaset/common/types";

export type ServerTierType = {
  name: string;
  services: string[];
  replicasetCount: number;
  instanceCount: number;
  rf: number;
  can_vote: boolean;
  replicasets: ServerReplicasetType[];
};

export type TierType = Override<
  ServerTierType,
  { replicasets: ReplicasetType[] }
>;
