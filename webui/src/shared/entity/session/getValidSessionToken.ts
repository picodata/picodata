import { CanceledError } from "axios";

import { sessionStore } from "shared/session/store";

import { refreshCurrentSession } from "./refresh";

const EXPIRY_MARGIN_MS = 5_000;

function needsRefresh(token: string) {
  try {
    const payload = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const { exp } = JSON.parse(atob(payload)) as { exp?: unknown };
    return (
      typeof exp !== "number" ||
      !Number.isFinite(exp) ||
      exp * 1000 <= Date.now() + EXPIRY_MARGIN_MS
    );
  } catch {
    return true;
  }
}

export async function getValidSessionToken(
  scope: number,
  signal?: AbortSignal
) {
  const ensureCurrent = () => {
    if (signal?.aborted || sessionStore.getSnapshot().scope !== scope) {
      throw new CanceledError("Session changed or request cancelled");
    }
  };

  ensureCurrent();
  const { auth } = sessionStore.getSnapshot().tokens;
  if (!auth || !needsRefresh(auth)) return auth;

  await refreshCurrentSession(scope);

  ensureCurrent();
  return sessionStore.getSnapshot().tokens.auth;
}
