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
