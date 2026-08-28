## fix/webui

- Fixed `instancesCurrentStateOffline` in `/api/v1/cluster` counting
  expelled instances as offline; it now counts only instances whose
  current state is actually `Offline`
