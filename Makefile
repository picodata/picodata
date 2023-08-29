.PHONY: default fmt lint test check fat clean benchmark

default: ;

install-cargo:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
		sh -s -- -y --profile default --default-toolchain 1.71.0

centos7-cmake3:
	if [ ! -L /usr/bin/cmake3 ] ; then \
		[ -f /usr/bin/cmake ] && sudo rm /usr/bin/cmake; \
		sudo ln -s /usr/bin/cmake3 /usr/bin/cmake; \
	fi
	sudo find {/opt,/usr} -name libgomp.spec -delete

reset-submodules:
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	git submodule update --init --recursive

tarantool-patch:
	echo "${VER_TNT}" > tarantool-sys/VERSION

build: tarantool-patch
	. ~/.cargo/env && \
	cargo build --locked

build-release: tarantool-patch
	. ~/.cargo/env && \
		cargo build --locked --release

install:
	mkdir -p $(DESTDIR)/usr/bin
	install -m 0755 target/*/picodata $(DESTDIR)/usr/bin/picodata

fmt:
	cargo fmt
	pipenv run fmt

lint:
	cargo fmt --check
	cargo check
	cargo clippy --version
	cargo clippy -- --deny clippy::all

	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" cargo doc --workspace --no-deps --document-private-items

	pipenv run lint

test:
	cargo test
	pipenv run pytest -n auto

check:
	@$(MAKE) lint --no-print-directory
	@$(MAKE) test --no-print-directory

fat:
	@$(MAKE) fmt --no-print-directory
	@$(MAKE) lint --no-print-directory
	@$(MAKE) test --no-print-directory

clean:
	cargo clean || true
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	find . -type d -name __pycache__ | xargs -n 500 rm -rf

benchmark:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/test_benchmark.py

flamegraph:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/test_benchmark.py --with-flamegraph

k6:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/sql/test_sql_perf.py
