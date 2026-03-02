import { z } from "zod";

export const groupByValue = z.enum(["INSTANCES", "TIERS"]);

export type TGroupByValue = z.infer<typeof groupByValue>;
