import axios, { AxiosError, AxiosRequestConfig } from "axios";

import { SessionModel } from "shared/session";

import { Credentials } from "../types";

export const sessionURL = "/api/v1/session";

export async function postSession({ username, password }: Credentials) {
  const response = await axios.post<SessionModel>(sessionURL, {
    username,
    password,
  });

  return response;
}

export function refreshSession(config: AxiosRequestConfig) {
  return axios.get<SessionModel>(sessionURL, config);
}

// TODO: enable when DELETE /session works
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const deleteSession = (config: AxiosRequestConfig) => {
  return Promise.resolve(); //TODO: make it as axios.delete(sessionURL, config);
};

export function isAuthError(e: AxiosError) {
  const status = e.status ?? e.response?.status ?? 0;

  return [401, 403].includes(status);
}
