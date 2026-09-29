## feat/plugin

- A plugin can write its own events to the instance audit log through `picodata_plugin::audit!`
  macro or by defining their own specialized one using `picodata_plugin::define_audit!`.
