## How to run
With `make`:
```bash
make test
```

With `cargo`:
```bash
cargo build -p tarantool-module-test-runner
cargo test
```

## Under the hood
When running `cargo test` some of the steps would be as you would expect:
- Unit tests and integration tests defined with `#[test]` macro
- Doc tests - code in documentation

But also it would execute our custom test runner which main fn is defined in `tests/run.rs`.
Then the steps would be the following:
1. Test runner starts `tarantool` with `run_tests.lua` script as an argument
2. `run_tests.lua` script does some initialization and loads `tarantool-module-test-runner` built as a dynamic library
3. Then `run_tests.lua` calls the main function of this module, which effectively is `start` in `tests/src/lib.rs`
4. `start` creates test spaces and calls `run_tests`
5. `run_tests` collects tests defined with `linkme::distributed_slice` macro from `tarantool` lib
6. `run_tests` collects tests that are explicitely added there by full path
7. `run_tests` feeds all tests to Rust default console test runner and collects result
8. result is reported through all the chain to the top and the process exits with appropriate exit code

All this is done as integration tests expect to be run from inside a `tarantool` instance.

## How to add tests
### If you don't need tarantool environment
Just add a test with `#[test]` macro.
You shouldn't use any `tarantool` symbols or symbols dependent on them in this case.

### Otherwise
#### Recommended
Use `linkme::distributed_slice` macro to add tests. See `tarantool::test` module docs for examples.

#### Deprecated
Insert the test directly by path into `run_tests` fn in `tests/src/lib.rs`

