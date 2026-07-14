## feat/webui

- Optimize `/api/v1/tiers` and `/api/v1/cluster` endpoints to reduce RPC calls.
  HTTP addresses are now read from `_pico_peer_address` storage instead of RPC,
  and memory info is only fetched from replicaset leaders. This reduces the
  number of RPC calls from O(N×RF) to O(N) where N is the number of replicasets.
  Offline instances now show their HTTP address (from storage) instead of empty string.
