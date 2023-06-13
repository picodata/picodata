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
	PATCH_DIR=$(pwd)/certification_patches/svace_patches
	(cd tarantool-sys/third_party/luajit; find ${PATCH_DIR} -name "luajit_*" | xargs -n 1 git apply)

build: tarantool-patch
	. ~/.cargo/env && \
	cargo build --locked

build-release: tarantool-patch
	. ~/.cargo/env && \
	cargo build --locked --release

install:
	mkdir -p $(DESTDIR)/usr/bin
	install -m 0755 target/*/picodata $(DESTDIR)/usr/bin/picodata
	mkdir -p $(DESTDIR)/usr/bin/picodata-libs
	cp -P \
		target/*/build/tarantool-sys/readline-prefix/lib/libreadline.* \
		$(DESTDIR)/usr/bin/picodata-libs
	cp -P \
		target/*/build/tarantool-sys/ncurses-prefix/lib/libtinfo.* \
		$(DESTDIR)/usr/bin/picodata-libs
	cp -P \
		target/*/build/tarantool-sys/zlib-prefix/lib/zlib.[^a]* \
		$(DESTDIR)/usr/bin/picodata-libs

fmt:
	cargo fmt
	pipenv run fmt

lint:
	cargo fmt --check
	cargo check
	cargo clippy --version
	cargo clippy -- --deny clippy::all --no-deps

	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" cargo doc --workspace --no-deps --document-private-items --exclude tlua --exclude sbroad-core --exclude tarantool

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
