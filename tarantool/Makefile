all:
	cargo build --all

test:
	cargo build -p tarantool-module-test-runner
	cargo test

test-pd:
	cargo build -p tarantool-module-test-runner --features=picodata
	TARANTOOL_EXECUTABLE=tarantool-pd cargo test

bench:
	cargo build -p perf-test --release
	TARANTOOL_MODULE_BUILD_MODE="release" tarantool perf-test/run.lua
