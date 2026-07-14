## feat

- [picodata#760] New configuration parameter `experimental_sharding_implementation` which
  enables the new behavior on the given tier. The parameter must be specified at
  cluster bootstrap via the configuration file and cannot be changed after that
  (in the future this restriction may be lifted).
