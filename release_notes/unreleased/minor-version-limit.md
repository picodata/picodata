## fix/cluster

- Joining or restarting an instance is now rejected if the resulting cluster
  would contain more than two Picodata minor versions. This prevents versions
  such as `25.5`, `26.0`, and `26.1` from coexisting during a rolling upgrade.
