import { useSyncExternalStore } from "react";

import { SessionModel, SessionStore } from "../model";
import { sessionStore } from "../store";

export function useSessionStore(): [SessionModel, SessionStore] {
  const { tokens, remembers } = useSyncExternalStore(
    sessionStore.subscribe,
    sessionStore.getSnapshot
  );

  return [
    tokens,
    {
      set: sessionStore.set,
      clear: sessionStore.clear,
      setRemembers: sessionStore.setRemembers,
      remembers,
    },
  ];
}

export function useSessionScope() {
  return useSyncExternalStore(sessionStore.subscribe, sessionStore.getSnapshot)
    .scope;
}
