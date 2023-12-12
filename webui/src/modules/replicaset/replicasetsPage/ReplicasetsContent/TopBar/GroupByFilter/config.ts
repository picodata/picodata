import { z } from "zod";

export const groupByValue = z.enum(["REPLICASETS", "INSTANCES", "TIERS"]);

export type TGroupByValue = z.infer<typeof groupByValue>;

export const groupByLabel: Record<TGroupByValue, string> = {
  INSTANCES: "Instances",
  REPLICASETS: "Replicasets",
  TIERS: "Tiers",
};

export const groupByOptions = [
  {
    label: groupByLabel.TIERS,
    value: "TIERS" as const,
  },
  {
    label: groupByLabel.REPLICASETS,
    value: "REPLICASETS" as const,
  },
  {
    label: groupByLabel.INSTANCES,
    value: "INSTANCES" as const,
  },
];
