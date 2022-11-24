all:
	cargo build --all

test:
	cargo build -p tarantool-module-test-runner
	cargo test

test-pd:
	cargo build -p tarantool-module-test-runner --features=picodata
	TARANTOOL_EXECUTABLE=tarantool-pd cargo test

benchmark:
	tests/run_benchmarks.lua
