## fix/sql

- `pico_instance_dir`, `pico_config_file_path` and `pico_instance_health_status`
  called with the uuid of another instance no longer crash the instance when it
  applies a change of the topology or of the schema while waiting for the reply
  ([!3843]).
- `pico_instance_health_status` called with the uuid of another instance that
  has no issues no longer fails to decode the reply ([!3843]).
