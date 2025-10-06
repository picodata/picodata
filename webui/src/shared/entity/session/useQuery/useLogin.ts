import { useMutation } from "react-query";
import { AxiosError } from "axios";
import { useContext } from "react";

import { useMutationConfig } from "shared/api";
import { useSessionStore, SessionModel } from "shared/session";
import { ConfigQueryContext } from "shared/entity/config";
import { ApiError } from "shared/api/errors";

import * as api from "../api";
import { SessionError, Credentials } from "../types";

export const useLogin = () => {
  const queryConfig = useMutationConfig();
  const [, session] = useSessionStore();
  const config = useContext(ConfigQueryContext);

  return useMutation<SessionModel, AxiosError<SessionError>, Credentials>({
    mutationKey: [api.postSession.name],
    mutationFn: (credentials) =>
      api.postSession(credentials).then((r) => r.data),
    onSuccess(value) {
      // This means that the app config is outdated
      // and auth is acutally disabled
      if (!value.auth) {
        config?.refetch();
      }

      session.set(value);
    },
    onError(error) {
      // This means that the app config is outdated
      // and auth is acutally disabled
      if (error.response?.data.error === ApiError.authDisabled) {
        config?.refetch();
      }
    },
    ...queryConfig,
  });
};
