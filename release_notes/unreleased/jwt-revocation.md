## feat/http

- Added `DELETE /api/v1/session` to revoke a refresh JWT cluster-wide. Revocations
  persist across restarts and are removed after token expiration plus 60 seconds.
  The endpoint rejects tokens identified as `auth` with `401` / `invalid token type`,
  regardless of signature validity or expiration. Other nonempty Bearer tokens
  receive `204 No Content`, regardless of token validity or user privileges.
  Existing access tokens expire normally.
  During a rolling upgrade, refresh keeps working and logout remains a no-op
  until the revocation table is created by the catalog migration.
