import { z } from "zod";

export const groupByValue = z.enum(["REPLICASETS", "INSTANCES", "TIERS"]);

export type TGroupByValue = z.infer<typeof groupByValue>;
