import { z } from "zod";

export const domainFilterValue = z.array(
  z.object({
    key: z.string(),
    value: z.array(z.string()),
  })
);

export const filterByValue = z.object({
  domain: domainFilterValue.optional(),
});

export type TFilterByValue = z.infer<typeof filterByValue>;
