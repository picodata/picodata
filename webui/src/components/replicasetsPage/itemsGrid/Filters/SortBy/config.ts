import { z } from "zod";

export const sortByValue = z.enum(["NAME", "FAILURE_DOMAIN"]);

export type TSortByValue = z.infer<typeof sortByValue>;

export const sortByLabel: Record<TSortByValue, string> = {
  NAME: "Name",
  FAILURE_DOMAIN: "Failure Domain",
};

export const sortByOptions = [
  {
    label: sortByLabel.NAME,
    value: "NAME" as const,
  },
  {
    label: sortByLabel.FAILURE_DOMAIN,
    value: "FAILURE_DOMAIN" as const,
  },
];
