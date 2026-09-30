import { SessionModel, SessionStore, sessionSchema } from "./model";

const key = "session";
const emptySession: SessionModel = { auth: "", refresh: "" };
const listeners = new Set<() => void>();
let localTokens = emptySession;
let remembers = true;
let snapshot = { tokens: emptySession, remembers, scope: 0 };

const sameTokens = (a: SessionModel, b: SessionModel) =>
  a.auth === b.auth && a.refresh === b.refresh;

function getSubject(token: string) {
  try {
    const payload = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const { sub } = JSON.parse(atob(payload)) as { sub?: unknown };
    return typeof sub === "string" ? sub : undefined;
  } catch {
    return undefined;
  }
}

function isSameSession(current: SessionModel, next: SessionModel) {
  if (!current.auth || !next.auth) return !current.auth && !next.auth;
  const currentSubject = getSubject(current.auth);
  const nextSubject = getSubject(next.auth);
  return (
    currentSubject !== undefined &&
    nextSubject !== undefined &&
    currentSubject === nextSubject
  );
}

function readTokens(): SessionModel {
  if (!remembers) return localTokens;
  try {
    const stored = localStorage.getItem(key);
    return stored ? sessionSchema.parse(JSON.parse(stored)) : emptySession;
  } catch {
    return emptySession;
  }
}

function getSnapshot() {
  const tokens = readTokens();
  if (
    !sameTokens(tokens, snapshot.tokens) ||
    remembers !== snapshot.remembers
  ) {
    snapshot = {
      tokens,
      remembers,
      scope: isSameSession(snapshot.tokens, tokens)
        ? snapshot.scope
        : snapshot.scope + 1,
    };
  }
  return snapshot;
}

function notify() {
  getSnapshot();
  listeners.forEach((listener) => listener());
}

function write(tokens: SessionModel, scope: number) {
  if (remembers) localStorage.setItem(key, JSON.stringify(tokens));
  else localTokens = tokens;
  snapshot = { tokens, remembers, scope };
  notify();
}

function onStorage(event: StorageEvent) {
  if (
    event.storageArea === localStorage &&
    (event.key === key || event.key === null)
  ) {
    notify();
  }
}

export const sessionStore = {
  getSnapshot,
  subscribe(listener: () => void) {
    listeners.add(listener);
    if (listeners.size === 1) window.addEventListener("storage", onStorage);
    return () => {
      listeners.delete(listener);
      if (!listeners.size) window.removeEventListener("storage", onStorage);
    };
  },
  set(tokens: SessionModel) {
    write(tokens, getSnapshot().scope + 1);
  },
  updateTokens(tokens: SessionModel, scope: number) {
    if (getSnapshot().scope !== scope) return false;
    write(tokens, scope);
    return true;
  },
  clear() {
    const scope = getSnapshot().scope + 1;
    localTokens = emptySession;
    if (remembers) localStorage.removeItem(key);
    snapshot = {
      tokens: emptySession,
      remembers,
      scope,
    };
    notify();
  },
  setRemembers: ((value) => {
    const next = typeof value === "function" ? value(remembers) : value;
    if (next === remembers) return;
    const tokens = readTokens();
    if (next) localStorage.setItem(key, JSON.stringify(tokens));
    else {
      localTokens = tokens;
      const stored = localStorage.getItem(key);
      try {
        if (stored) {
          const parsed = sessionSchema.safeParse(JSON.parse(stored));
          if (parsed.success && sameTokens(parsed.data, tokens))
            localStorage.removeItem(key);
        }
      } catch {}
    }
    remembers = next;
    notify();
  }) satisfies SessionStore["setRemembers"],
};
