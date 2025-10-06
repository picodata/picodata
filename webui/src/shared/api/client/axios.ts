import axios, { AxiosError, AxiosRequestConfig } from "axios";
import { useContext, useMemo } from "react";

import { useAppConfig } from "shared/entity/config";
import { RefreshContext, isAuthError, sessionURL } from "shared/entity/session";
import { useSessionStore } from "shared/session";

export const useAuthAxios = () => {
  const [tokens] = useSessionStore();
  const refresh = useContext(RefreshContext);
  const config = useAppConfig();

  const authAxios = useMemo(() => {
    if (!config.isAuthEnabled) {
      return axios;
    }

    const instance = axios.create(withToken(() => tokens.auth));
    // Use axios interceptor instead of QueryConfig.onError or QueryConfig.retry
    // because we need the original request semantics to be intact
    // plus make it appear as if the request has succeeded when refresh happens
    instance.interceptors.response.use(
      (value) => {
        return value;
      },
      (e: AxiosError) => {
        // If we're dealing with user session or a non-auth error, or just can't refresh - do nothing
        if (e.config?.url === sessionURL || !isAuthError(e) || !refresh) {
          throw e;
        }

        // otherwise, try to refresh the token
        return refresh.mutateAsync().then((r) => {
          if (!e.config) {
            throw new Error(
              `Cannot retry ${JSON.stringify(
                e.request,
                null,
                2
              )}: no request config`
            );
          }

          // and retry the request with the new auth token
          return instance.request(withToken(() => r.auth, e.config));
        });
      }
    );

    return instance;
  }, [config.isAuthEnabled, refresh, tokens.auth]);

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
