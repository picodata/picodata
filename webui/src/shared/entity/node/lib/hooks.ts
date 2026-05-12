import { useMemo } from "react";

import { ServerTierType } from "../../tier";
import { TierMemory } from "../../memory";

import { getNodes } from "./utils";

export const useNodes = (
  initialTiers: ServerTierType[] | undefined,
  memory?: TierMemory[] | undefined
) => {
  return useMemo(() => getNodes(initialTiers, memory), [initialTiers, memory]);
};
