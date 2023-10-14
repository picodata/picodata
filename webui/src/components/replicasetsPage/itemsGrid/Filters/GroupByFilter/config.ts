import { z } from "zod";

export const groupByValue = z.enum(["REPLICASETS", "INSTANCES"]);

export type TGroupByValue = z.infer<typeof groupByValue>;

export const groupByLabel: Record<TGroupByValue, string> = {
  INSTANCES: "Instances",
  REPLICASETS: "Replicasets",
};

export const groupByOptions = [
  {
    label: groupByLabel.INSTANCES,
    value: "INSTANCES" as const,
  },
  {
    label: groupByLabel.REPLICASETS,
    value: "REPLICASETS" as const,
  },
];
