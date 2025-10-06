import { createContext } from "react";

import { useRefresh } from "./useQuery/useRefresh";

export const RefreshContext = createContext<ReturnType<
  typeof useRefresh
> | null>(null);
