import { AxiosError, CanceledError, isAxiosError } from "axios";

import { sessionStore } from "shared/session/store";
import { SessionModel } from "shared/session/model";

import { isAuthError, refreshSession } from "./api";

let pending: { scope: number; promise: Promise<SessionModel> } | undefined;

export function refreshCurrentSession(scope: number): Promise<SessionModel> {
  const current = sessionStore.getSnapshot();
  if (current.scope !== scope) {
    return Promise.reject(new CanceledError("Session is no longer available"));
  }
  if (!current.tokens.refresh) {
    sessionStore.clear();
    return Promise.reject(new AxiosError("Session expired", "SESSION_EXPIRED"));
  }
  if (pending?.scope === scope) return pending.promise;

  const promise = refreshSession({
    headers: { Authorization: `Bearer ${current.tokens.refresh}` },
  })
    .then(({ data }) => {
      if (!sessionStore.updateTokens(data, scope)) {
        throw new CanceledError("Session changed during refresh");
      }
      return data;
    })
    .catch((error: unknown) => {
      if (
        sessionStore.getSnapshot().scope === scope &&
        isAxiosError(error) &&
        isAuthError(error)
      ) {
        sessionStore.clear();
      }
      throw error;
    })
    .finally(() => {
      if (pending?.promise === promise) pending = undefined;
    });
  pending = { scope, promise };
  return promise;
}
