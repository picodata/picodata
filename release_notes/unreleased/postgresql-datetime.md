## feat/sql

- Datetime parsing has been aligned with PostgreSQL, and many new formats are
  now available, for example:
  - `2017-09-24 02:59:59`
  - `2017-09-24 02:59`
  - `2017-09-24 02:59:59 +03:00`
  - `24 Sep 2017`
  - `Sun Sep 24 02:59:59 2017 MSK`
  - `20170924`
  - `2017-09-24 2:59:59 PM`
- Datetimes in bind parameters and constant casts are now parsed the same way
  as on the storages.
- `TO_DATE` with an empty format accepts the same formats as a cast to
  `DATETIME`, so `10/11/2025` is read as October 11.
- Time zone names such as `Europe/Moscow` and abbreviations such as `MSK` are
  resolved with the system time zone database, so the `picodata` package now
  depends on `tzdata`.
