## feat/sql

- New scalar function `format`, as in PostgreSQL: `format('%I = %L', 'My
  Col', 'it''s')` gives `"My Col" = 'it''s'`. It supports the specifiers
  `%s`, `%I`, `%L` and `%%`, and makes values of any type into text as
  `::text` does.
