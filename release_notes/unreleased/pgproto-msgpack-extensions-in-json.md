## fix/pgproto

- Over pgproto, `SELECT opts FROM _pico_index` for a vinyl index created with
  `WITH (bloom_fpr = 0.001)` used to return

      [{"unique":true},{"bloom_fpr":[1,[3,28]]}]

  and now returns

      [{"unique":true},{"bloom_fpr":"0.001"}]

  DECIMAL, UUID and DATETIME values nested in a column of type `any`, `map` or
  `array` were sent as raw msgpack extension byte arrays, since json has no
  type of its own for any of them. They are now rendered as json strings.
