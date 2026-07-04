all: build

# The `mock` feature is enabled automatically for tests and benches by each
# crate's self-referential dev-dependency, so no feature flags are needed.
TARGET_ROOT=target
SQL_CRATES = -p sql-ir -p sql-executor -p sql-frontend -p sql-planner

build:
	cargo build --release

build_debug:
	cargo build

bench:
	make clean
	cargo bench $(SQL_CRATES)

bench_check:
	make clean
	cargo bench $(SQL_CRATES) --no-run

clean:
	rm -rf $(TARGET_ROOT)/release/libsql*
	rm -rf $(TARGET_ROOT)/release/build/libsql*
	rm -rf $(TARGET_ROOT)/release/deps/libsql*
	rm -rf $(TARGET_ROOT)/release/incremental/libsql*
	rm -rf $(TARGET_ROOT)/debug/libsql*
	rm -rf $(TARGET_ROOT)/debug/build/libsql*
	rm -rf $(TARGET_ROOT)/debug/deps/libsql*
	rm -rf $(TARGET_ROOT)/debug/incremental/libsql*

test:
	cargo test $(SQL_CRATES)

test_all: test bench_check
