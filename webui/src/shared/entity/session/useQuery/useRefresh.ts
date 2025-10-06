import { AxiosError } from "axios";
import { useMutation } from "react-query";

import { SessionModel, useSessionStore } from "shared/session";
import { useMutationConfig, withToken } from "shared/api";

import * as api from "../api";
import { SessionError } from "../types";

export const useRefresh = () => {
  const queryConfig = useMutationConfig();
  const [tokens, session] = useSessionStore();

  return useMutation<SessionModel, AxiosError<SessionError>>({
    mutationKey: [api.refreshSession.name, tokens.refresh],
    mutationFn: () =>
      api.refreshSession(withToken(() => tokens.refresh)).then((r) => r.data),
    onSuccess(value) {
      session.set(value);
    },
    ...queryConfig,
  });
};
