import { AxiosError } from "axios";
import { useMutation } from "react-query";
import { useEffect } from "react";

import { SessionModel, useSessionScope } from "shared/session";
import { useMutationConfig } from "shared/api";

import { refreshCurrentSession } from "../refresh";
import { SessionError } from "../types";

export const useRefresh = () => {
  const queryConfig = useMutationConfig();
  const scope = useSessionScope();

  const mutation = useMutation<SessionModel, AxiosError<SessionError>, number>({
    mutationKey: ["refreshSession", scope],
    mutationFn: refreshCurrentSession,
    ...queryConfig,
  });
  const { reset } = mutation;
  useEffect(() => reset(), [scope, reset]);

  return {
    ...mutation,
    error: mutation.variables === scope ? mutation.error : null,
    isLoading: mutation.variables === scope && mutation.isLoading,
    mutateAsync: () => mutation.mutateAsync(scope),
  };
};
