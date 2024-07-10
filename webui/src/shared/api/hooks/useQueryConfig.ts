import { retryDelay, retry } from "../utils/retry";

// потенциально в конфиге появится логика и станет полноценным хуком
export const useQueryConfig = () => {
  return {
    refetchOnWindowFocus: false,
    refetchOnMount: false,
    refetchOnReconnect: true,
    networkMode: "online" as const,
    retryDelay,
    retry,
  };
};
