## fix

- Implement and export `box_session_user_name` function in tarantool-sys ([tarantool!387]).
- Implement a function `tarantool::session::user_name` and use it in the `on_access_denied` trigger to prevent an implicit multi-statement transaction, which panicked ([!3196]).
