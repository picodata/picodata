import { Override } from "../../../utils/tsUtils";

export type ServerInstanceType = {
  name: string;
  targetState: "Online" | "Offline" | "Expelled";
  currentState: "Online" | "Offline" | "Expelled";
  failureDomain: Record<string, string>;
  version: string;
  isLeader: boolean;
  pgAddress: string;
  binaryAddress: string;
  httpAddress?: string;
};

export type InstanceType = Override<
  ServerInstanceType,
  { failureDomain: Array<{ key: string; value: string }> }
>;
