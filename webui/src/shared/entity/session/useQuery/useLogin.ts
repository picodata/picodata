import { useMutation } from "react-query";
import { AxiosError, CanceledError } from "axios";
import { useContext } from "react";

import { useMutationConfig } from "shared/api";
import { SessionModel } from "shared/session";
import { sessionStore } from "shared/session/store";
import { ConfigQueryContext } from "shared/entity/config";
import { ApiError } from "shared/api/errors";

import * as api from "../api";
import { SessionError, Credentials } from "../types";

export const useLogin = () => {
  const queryConfig = useMutationConfig();
  const config = useContext(ConfigQueryContext);

  return useMutation<SessionModel, AxiosError<SessionError>, Credentials>({
    mutationKey: [api.postSession.name],
    mutationFn: async (credentials) => {
      const { scope } = sessionStore.getSnapshot();
      const { data } = await api.postSession(credentials);
      if (sessionStore.getSnapshot().scope !== scope) {
        throw new CanceledError("Session changed during login");
      }
      sessionStore.set(data);
      return data;
    },
    onSuccess(value) {
      // This means that the app config is outdated
      // and auth is acutally disabled
      if (!value.auth) {
        config?.refetch();
      }
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
