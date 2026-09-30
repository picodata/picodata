export type WebuiTheme = "light" | "dark";

export type Unsubscribe = () => void;

export type WebuiSessionErrorCode = "SESSION_EXPIRED" | "REFRESH_FAILED";

export interface WebuiSessionError extends Error {
  readonly code: WebuiSessionErrorCode;
}

export interface WebuiSdk {
  readonly version: string;
  readonly apiBaseUrl: string;
  readonly theme: WebuiTheme;
  getLocale(): string;
  onThemeChange(listener: (theme: WebuiTheme) => void): Unsubscribe;
  onLocaleChange(listener: (locale: string) => void): Unsubscribe;
  getSessionToken(): string;
  refreshSession(): Promise<string>;
  reportFatalError(error: unknown): void;
}

export interface WebuiPageElement extends HTMLElement {
  webuiSdk: WebuiSdk;
}
