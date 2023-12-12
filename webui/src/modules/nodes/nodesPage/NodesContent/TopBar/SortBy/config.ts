import { z } from "zod";

export const sortByValue = z.enum(["NAME", "FAILURE_DOMAIN"]);
export const sortOrderValue = z.enum(["ASC", "DESC"]);
export const sortValue = z.object({
  by: sortByValue,
  order: sortOrderValue,
});

export type TSortByValue = z.infer<typeof sortByValue>;
export type TSortValue = z.infer<typeof sortValue>;

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

export const DEFAULT_SORT_ORDER = "ASC";
export const DEFAULT_SORT_BY = sortByOptions[0].value;
