import { version } from "@picodata/ui-kit/package.json";

import type {
  Unsubscribe,
  WebuiSdk,
  WebuiTheme,
  WebuiSessionErrorCode,
} from "@picodata/ui-kit";
import { sessionStore } from "shared/session/store";
import { refreshCurrentSession } from "shared/entity/session/refresh";

type Listener<T> = (value: T) => void;

function sessionError(code: WebuiSessionErrorCode) {
  return Object.assign(new Error(code), { code });
}

export function createWebuiSdk(
  initialTheme: WebuiTheme,
  initialLocale: string,
  onFatal: (error: unknown) => void
) {
  let disposed = false;
  let theme = initialTheme;
  let locale = initialLocale;
  const scope = sessionStore.getSnapshot().scope;
  const themes = new Set<Parameters<WebuiSdk["onThemeChange"]>[0]>();
  const locales = new Set<Parameters<WebuiSdk["onLocaleChange"]>[0]>();
  const isCurrent = () =>
    !disposed && sessionStore.getSnapshot().scope === scope;
  const subscribe = <T>(
    listeners: Set<Listener<T>>,
    listener: Listener<T>
  ): Unsubscribe => {
    if (!disposed) listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  };
  const notify = <T>(listeners: Set<Listener<T>>, value: T) => {
    listeners.forEach((listener) => {
      try {
        listener(value);
      } catch (error) {
        if (!disposed) onFatal(error);
      }
    });
  };
  const sdk: WebuiSdk = {
    version,
    apiBaseUrl: "",
    get theme() {
      return theme;
    },
    getLocale: () => locale,
    getSessionToken: () =>
      isCurrent() ? sessionStore.getSnapshot().tokens.auth : "",
    onThemeChange: (listener) => subscribe(themes, listener),
    onLocaleChange: (listener) => subscribe(locales, listener),
    refreshSession: async () => {
      if (!isCurrent()) throw sessionError("SESSION_EXPIRED");
      try {
        const tokens = await refreshCurrentSession(scope);
        if (!isCurrent()) throw sessionError("SESSION_EXPIRED");
        return tokens.auth;
      } catch {
        const expired =
          !isCurrent() || !sessionStore.getSnapshot().tokens.refresh;
        throw sessionError(expired ? "SESSION_EXPIRED" : "REFRESH_FAILED");
      }
    },
    reportFatalError: (error) => {
      if (!disposed) onFatal(error);
    },
  };
  return {
    sdk,
    update(nextTheme: WebuiTheme, nextLocale: string) {
      if (disposed) return;
      const themeChanged = theme !== nextTheme;
      const localeChanged = locale !== nextLocale;
      theme = nextTheme;
      locale = nextLocale;
      if (themeChanged) notify(themes, theme);
      if (localeChanged) notify(locales, locale);
    },
    dispose() {
      disposed = true;
      themes.clear();
      locales.clear();
    },
  };
}
