import { Override } from "../../../utils/tsUtils";
import { NodeType } from "../../tier";

export type InstanceState = "Online" | "Offline" | "Expelled";

export type UpstreamStatus =
  | "follow"
  | "stopped"
  | "disconnected"
  | "off"
  | "connect"
  | "connected"
  | "auth"
  | "ready"
  | "final_join"
  | "joined"
  | "sync"
  | "loading"
  | "fetch_snapshot"
  | "fetched_snapshot"
  | "wait_snapshot"
  | "register"
  | "registered";

export type DownstreamStatus = "follow" | "stopped";

export type Stream = {
  idle?: number | null;
  lag?: number | null;
};
export type UpStream = Stream & {
  status: UpstreamStatus;
  peer?: string | null;
  message?: string | null;
};
export type DownStream = Stream & {
  status: DownstreamStatus;
  vclock?: Record<string, number> | null;
};

export type InstanceLog = {
  destination?: string | null;
  format?: string;
  level?: string;
};
export type InstanceReplication = {
  id: number;
  uuid: string;
  lsn?: number;
  upstream?: UpStream | null;
  downstream?: DownStream | null;
};

export type InstanceVinyl = {
  bloomFpr?: number;
  cache?: string;
  maxTupleSize?: string;
  memory?: string;
  pageSize?: string;
  rangeSize?: string;
  readThreads?: number;
  runCountPerLevel?: number;
  runSizeRatio?: number;
  timeout?: number;
  writeThreads?: number;
};
export type InstanceMemtx = {
  memory?: string;
  systemMemory?: string;
  maxTupleSize?: string;
};

export type ServerInstanceType = {
  name: string;
  targetState: InstanceState;
  currentState: InstanceState;
  failureDomain: Record<string, string>;
  version: string;
  isLeader: boolean;
  pgAddress: string;
  binaryAddress: string;
  httpAddress?: string;
  isRaftLeader?: boolean;
  isVoter?: boolean;
  uuid: string;
};

export type InstanceType = Override<
  ServerInstanceType,
  { failureDomain: Array<{ key: string; value: string }> }
>;
export type InstanceNodeType = InstanceType & {
  syntheticId: string;
  type: NodeType.Instance;
};

export type FullInstance = {
  log?: InstanceLog;
  replication?: Record<number, InstanceReplication>;
  vinyl?: InstanceVinyl;
  memtx?: InstanceMemtx;
  adminSocket?: string;
  backupDir?: string;
  targetState: InstanceState;
  currentState: InstanceState;
  instanceDir?: string;
  name?: string;
  picodataVersion?: string;
  raftId?: number;
  replicasetName?: string;
  replicasetUuid?: string;
  shareDir?: string;
  tier?: string;
  uuid?: string;
};
