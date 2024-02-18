import { z } from "zod";

export const groupByValue = z.enum(["USERS", "ROLES"]);

export type TGroupByValue = z.infer<typeof groupByValue>;

export const groupByLabel: Record<TGroupByValue, string> = {
  USERS: "Users",
  ROLES: "Roles",
};

export const groupByOptions = [
  {
    label: groupByLabel.USERS,
    value: "USERS" as const,
  },
  {
    label: groupByLabel.ROLES,
    value: "ROLES" as const,
  },
];
