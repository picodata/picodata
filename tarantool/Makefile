all:
	cargo build --all

test:
	cargo build -p tarantool-module-test-runner
	tests/test.sh

test-pd:
	cargo build -p tarantool-module-test-runner --features=picodata
	TARANTOOL_EXECUTABLE=tarantool-pd tests/test.sh

benchmark:
	tests/run_benchmarks.lua
