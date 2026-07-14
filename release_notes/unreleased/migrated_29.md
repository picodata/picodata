## fix

- Fixed crash when disabling a plugin with slow background jobs. Previously,
  if a job didn't finish within the shutdown timeout, the plugin library could
  be unloaded while the job fiber was still running.
