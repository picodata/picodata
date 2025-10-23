all: build

TARGET_ROOT=target

build:
	cargo build --release

build_debug:
	cargo build

bench:
	make clean
	cargo bench --features mock

bench_check:
	make clean
	cargo bench --features mock --no-run

clean:
	rm -rf $(TARGET_ROOT)/release/libsbroad*
	rm -rf $(TARGET_ROOT)/release/build/libsbroad*
	rm -rf $(TARGET_ROOT)/release/deps/libsbroad*
	rm -rf $(TARGET_ROOT)/release/incremental/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/build/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/deps/libsbroad*
	rm -rf $(TARGET_ROOT)/debug/incremental/libsbroad*

test:
	cargo test --features mock

test_all: test bench_check
