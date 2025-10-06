export const enum ApiError {
  wrongCredentials = "wrongCredentials",
  sessionExpired = "sessionExpired",
  authDisabled = "authDisabled",
}

export type ApiErrors = Record<ApiError, string>;
