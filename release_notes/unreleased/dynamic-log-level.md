## feat/config

##### picodata log level can now be changed dynamically

You can change the log level of a picodata instance by using new SQL syntax:

```sql
ALTER SYSTEM SET LOCAL log_level = 'warn';
ALTER SYSTEM SET LOCAL log_level TO 'warn'; -- alternative syntax
```

This command will update the log level of an instance without needing to change the config file or restarting.
The config change is volatile and will not persist between restarts.

You can also restore the log level configured via the configuration file using this command:

```sql
ALTER SYSTEM RESET LOCAL log_level;
ALTER SYSTEM SET LOCAL log_level TO DEFAULT; -- alternative syntax
```

###### Retrieving current configuration

You can also retrieve the currently effective log level using the new `pico_log_level` SQL function:

```sql
SELECT pico_log_level();

> 'info'
```

You can also retrieve the list of all defined log levels and their numerical values (larger numbers are more verbose):

```sql
SELECT pico_log_level_map();

> {"fatal":0,"system":1,"error":2,"crit":3,"warn":4,"info":5,"verbose":6,"debug":7}
```
