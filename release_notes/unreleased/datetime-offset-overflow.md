## fix/plugin

- Fixed datetime SQL parameters passed from plugins with a UTC offset beyond
  ±09:06, for example `+10:00` or `-12:00`: the offset is no longer replaced
  with a wrong one.
