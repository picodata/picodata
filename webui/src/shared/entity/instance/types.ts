import { Override } from "../../utils/tsUtils";

export type ServerInstanceType = {
  name: string;
  targetGrade: string;
  currentGrade: string;
  failureDomain: Record<string, string>;
  version: string;
  isLeader: boolean;
};

export type InstanceType = Override<
  ServerInstanceType,
  { failureDomain: Array<{ key: string; value: string }> }
>;
