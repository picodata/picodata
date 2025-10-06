import { UseMutationOptions } from "react-query";

import { retryDelay } from "../utils/retry";

// потенциально в конфиге появится логика и станет полноценным хуком
export const useMutationConfig = () => {
  return {
    retryDelay: retryDelay,
    retry: false,
  } satisfies UseMutationOptions;
};
