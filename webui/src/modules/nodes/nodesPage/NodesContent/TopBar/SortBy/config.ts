import { z } from "zod";

export const sortByValue = z.enum(["NAME", "FAILURE_DOMAIN"]);
export const sortOrderValue = z.enum(["ASC", "DESC"]);
export const sortValue = z.object({
  by: sortByValue,
  order: sortOrderValue,
});

export type TSortByValue = z.infer<typeof sortByValue>;
export type TSortValue = z.infer<typeof sortValue>;

export const DEFAULT_SORT_ORDER = "ASC" as const;
export const DEFAULT_SORT_BY = "NAME" as const;
