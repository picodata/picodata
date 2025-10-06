import { useCallback, useMemo, useState } from "react";

import {
  LsStateOptions,
  useLsState,
  getLsValue,
} from "shared/localStorage/hooks/useLsState";

import { SessionModel, sessionSchema, SessionStore } from "../model";

const key = "session";
const cleanSlate: SessionModel = {
  auth: "",
  refresh: "",
};
const lsStateOptions = {
  key,
  schema: sessionSchema,
  defaultValue: cleanSlate,
} satisfies LsStateOptions<typeof sessionSchema>;

export function useSessionStore(): [SessionModel, SessionStore] {
  const [localTokens, setLocalTokens] = useState(cleanSlate);
  const [storedTokens, setTokens] = useLsState(lsStateOptions);
  const [remembers, setRemembers] = useState(true);

  const set = useCallback(
    (value: SessionModel) => (remembers ? setTokens : setLocalTokens)(value),
    [remembers, setTokens]
  );
  const clear = useCallback(() => set(cleanSlate), [set]);

  const tokens = useMemo(
    () => (remembers ? storedTokens : localTokens),
    [localTokens, remembers, storedTokens]
  );

  return [tokens, { clear, set, setRemembers, remembers }];
}

useSessionStore.getState = () => {
  return getLsValue(lsStateOptions);
};
