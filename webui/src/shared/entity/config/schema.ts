import { z } from "zod";

export const WebUIConfigSchema = z.strictObject({
  isAuthEnabled: z.boolean(),
});
