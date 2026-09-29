## feat/audit

- To accommodate plugins being able to write to audit log, events now include a new field called `subsystem`. Core
  picodata events have it set to `picodata`, while plugin-written events put the plugin name there, allowing developers
  to keep separate event `title` namespaces.

```json lines
// picodata event (subsystem="picodata")
{"id":"1.0.1","time":"2026-09-29T13:28:01.654+0300","message":"audit log is ready","subsystem":"picodata","title":"init_audit","severity":"low"}
// plugin event (subsystem="shavery")
{"id":"1.0.15","time":"2026-09-29T13:28:02.792+0300","message":"shaved yak `1`","subsystem":"shavery","title":"shave_yak","severity":"high","name":"1","initiator":"shaver"}
```
