import type z from "zod";

import type { WebUIConfigSchema } from "./schema";

export type WebUIConfig = z.infer<typeof WebUIConfigSchema>;
