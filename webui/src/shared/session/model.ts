import { Dispatch, SetStateAction } from "react";
import { z } from "zod";

export const sessionSchema = z.strictObject({
  auth: z.string(),
  refresh: z.string(),
});

export type SessionModel = z.infer<typeof sessionSchema>;

export interface SessionStore {
  clear(): void;
  set(newTokens: z.infer<typeof sessionSchema>): void;
  setRemembers: Dispatch<SetStateAction<boolean>>;
  remembers: boolean;
}
