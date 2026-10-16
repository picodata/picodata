## feat/acl

- Removed the limit on the number of roles. Before, users and roles
  together were limited to 128.
- Raised the limit on the number of users from 128 to 512. Builtin users
  count against it, roles do not. 27 places are kept for the system
  users of later versions, so 482 users can be created. When the limit
  is reached, `CREATE USER` fails with an error.
