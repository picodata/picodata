import { useMutation } from "react-query";
import { AxiosError } from "axios";
import { useNavigate } from "react-router";

import { useMutationConfig, withToken } from "shared/api";
import { useSessionStore } from "shared/session";
import { sessionStore } from "shared/session/store";
import { Routes } from "shared/router/config";

import * as api from "../api";
import { SessionError } from "../types";

export const useLogout = () => {
  const queryConfig = useMutationConfig();
  const [tokens] = useSessionStore();
  const navigate = useNavigate();

  return useMutation<void, AxiosError<SessionError>>({
    mutationKey: api.deleteSession.name,
    mutationFn: () => {
      sessionStore.clear();
      navigate(Routes.LOGIN);
      return api.deleteSession(withToken(() => tokens.auth));
    },
    ...queryConfig,
  });
};
