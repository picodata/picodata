## breaking

- Removed the deprecated `-a`/`--auth-type` option from `picodata expel`.
  The authentication method is now always detected automatically.
- Removed the deprecated `.proc_cas`, `.proc_runtime_info`,
  `.proc_enable_all_plugins`, and `.proc_update_instance` RPC procedures.
  Use `.proc_cas_v2`, `.proc_runtime_info_v2`, `.proc_before_online`, and
  `.proc_update_instance_v2`, respectively.
- Removed the deprecated SQL function `instance_uuid()`. Use
  `pico_instance_uuid()` instead.
- Removed compatibility shims for rolling upgrades from Picodata 25.5.x and
  for system catalogs older than 25.5.3.
