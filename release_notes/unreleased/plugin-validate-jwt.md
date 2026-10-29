## feat/plugin

- A plugin can validate a WebUI JWT through `picodata_plugin::authentication::validate_jwt`.
  It accepts a bare token or the whole `Authorization` header value and returns the user name
  and roles from the token. Checking the roles is up to the plugin. When WebUI auth is disabled
  (`jwt_secret` is empty), it returns `None`. Expired tokens fail with the new `JwtExpired` error
  code, so a client knows to refresh its session. Malformed, forged and refresh tokens fail with
  the new `JwtInvalid` code.
