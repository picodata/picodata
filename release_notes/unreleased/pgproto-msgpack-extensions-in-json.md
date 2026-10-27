## fix/pgproto

- Over pgproto, `SELECT opts FROM _pico_index` for a vinyl index created with
  `WITH (bloom_fpr = 0.001)` used to return

      [{"unique":true},{"bloom_fpr":[1,[3,28]]}]

  and now returns

      [{"unique":true},{"bloom_fpr":0.001}]

  DECIMAL, UUID and DATETIME values nested in a column of type `any`, `map` or
  `array` were sent as raw msgpack extension byte arrays. They are now rendered
  the way postgres renders the matching type inside json: a DECIMAL as a number
  keeping every digit, a UUID and a DATETIME as strings. Note that most clients
  parse a json number into a float unless explicitly configured not to, e.g.
  with psycopg's `set_json_loads(partial(json.loads, parse_float=Decimal))`, so
  digits beyond float precision are lost on the client side.
