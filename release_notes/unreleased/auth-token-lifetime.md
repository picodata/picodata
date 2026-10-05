## feat/http

- Added the `jwt_auth_token_lifetime` (default 1800 seconds) and
  `jwt_refresh_token_lifetime` (default 432000 seconds) parameters, which
  configure the HTTP API token lifetimes via `ALTER SYSTEM`. The `refresh`
  token lifetime must be greater than the `auth` token lifetime.
  The default token lifetimes are now shorter: 30 minutes instead of
  24 hours for the `auth` token and 120 hours instead of 1 year for the
  `refresh` token. This strengthens security and is a prerequisite for
  the upcoming `refresh` token revocation mechanism.

----

Affects scripts polling authenticated endpoints such as
`/api/v1/health/status` and `/api/v1/instance/:uuid` with a long-lived
token. Unauthenticated endpoints (`/api/v1/health/{live,ready,startup}`,
`/metrics`) are not affected.
