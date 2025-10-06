import { ApiError } from "shared/api/errors";

export interface Credentials {
  username: string;
  password: string;
}

export interface SessionError {
  error: ApiError;
  errorMessage: string;
}
