import { isAxiosError } from "axios";

import { MAX_REQUEST_RETRY_COUNT, RETRY_TIME_TIME_MS_RATIO } from "./constants";

export const retry = (failureCount: number, error: unknown) => {
  if (isAxiosError(error)) {
    if (error.status) {
      const isClientError = error.status >= 400 && error.status < 500;

      return !isClientError;
    }
  }

  return failureCount <= MAX_REQUEST_RETRY_COUNT;
};

export const retryDelay = (failureCount: number) =>
  (failureCount + 1) * RETRY_TIME_TIME_MS_RATIO;
