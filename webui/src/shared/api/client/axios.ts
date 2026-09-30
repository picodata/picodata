import axios, {
  AxiosError,
  AxiosHeaders,
  AxiosRequestConfig,
  CanceledError,
  create,
} from "axios";
import { useContext, useMemo } from "react";

import { useAppConfig } from "shared/entity/config";
import { RefreshContext, sessionURL } from "shared/entity/session";
import { useSessionScope, useSessionStore } from "shared/session";
import { sessionStore } from "shared/session/store";

export const useAuthAxios = () => {
  const [tokens] = useSessionStore();
  const scope = useSessionScope();
  const refresh = useContext(RefreshContext);
  const config = useAppConfig();

  const authAxios = useMemo(() => {
    if (!config.isAuthEnabled) {
      return axios;
    }

    const instance = create(withToken(() => tokens.auth));
    // Use axios interceptor instead of QueryConfig.onError or QueryConfig.retry
    // because we need the original request semantics to be intact
    // plus make it appear as if the request has succeeded when refresh happens
    instance.interceptors.response.use(
      (value) => {
        return value;
      },
      (e: AxiosError) => {
        // If we're dealing with user session or a non-auth error, or just can't refresh - do nothing
        const request = e.config as typeof e.config & {
          sessionRetried?: boolean;
        };
        if (
          !request ||
          request.url === sessionURL ||
          e.response?.status !== 401 ||
          !refresh ||
          request.sessionRetried
        ) {
          throw e;
        }
        if (
          sessionStore.getSnapshot().scope !== scope ||
          request.signal?.aborted
        ) {
          throw new CanceledError("Session changed or request cancelled");
        }
        request.sessionRetried = true;

        const current = sessionStore.getSnapshot().tokens;
        const tokenChanged =
          current.auth &&
          AxiosHeaders.from(request.headers).get("Authorization") !==
            `Bearer ${current.auth}`;
        const refreshed = tokenChanged
          ? Promise.resolve(current)
          : refresh.mutateAsync();
        return refreshed.then((r) => {
          if (
            sessionStore.getSnapshot().scope !== scope ||
            request.signal?.aborted
          ) {
            throw new CanceledError("Session changed or request cancelled");
          }
          // and retry the request with the new auth token
          return instance.request(withToken(() => r.auth, request));
        });
      }
    );

    return instance;
  }, [config.isAuthEnabled, refresh, tokens.auth, scope]);

  return authAxios;
};

export function withToken<H extends AxiosRequestConfig>(
  token: () => string,
  config?: H
) {
  return {
    ...config,
    headers: {
      ...config?.headers,
      get Authorization() {
        return `Bearer ${token()}`;
      },
    },
  } satisfies AxiosRequestConfig;
}
