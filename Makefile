# Cargo supports posix jobserver protocol to restrict its parallelism.
# XXX: we don't need to propagate -jN if N > 1, make jobserver got us covered.
# https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html
MAKE_JOBSERVER_ARGS = $(filter -j%, $(MAKEFLAGS))
ifneq ($(MAKE_JOBSERVER_ARGS),-j1)
MAKE_JOBSERVER_ARGS =
endif

# Select appropriate pytest parallelism flags.
PYTEST_NUMPROCESSES = $(patsubst -j%, --numprocesses=%, $(filter -j%, $(MAKEFLAGS)))
ifeq ($(PYTEST_NUMPROCESSES),)
PYTEST_NUMPROCESSES = --numprocesses=auto
endif

# It should be possible to keep the flags to the bare minimum.
# Hence, we don't use `override` here but add it to all build prerequisites.
CARGO_FLAGS := --features webui --all

PYTEST_FLAGS :=

# For clarity and an ability to turn it off without overriding whole CARGO_FLAGS
ERROR_INJECTION := --features error_injection

# Devs may want to drop this flag using make LOCKED=
LOCKED := --locked

.PHONY: default
default: ;

.PHONY: tarantool-patch
tarantool-patch:
	if test ! -f tarantool-sys/VERSION || \
	   test "${VER_TNT}" != "$(cat tarantool-sys/VERSION)"; then \
		echo "${VER_TNT}" > tarantool-sys/VERSION; \
	fi

# XXX: All targets but build-release-pkg should trigger this one!
# XXX: We can't use CARGO_FLAGS here since it's a standalone project w/o profile definitions etc.
.PHONY: build-plug-wrong-version
build-plug-wrong-version:
	cd test/plug_wrong_version && \
		$(CARGO_ENV) \
		cargo build $(LOCKED) $(MAKE_JOBSERVER_ARGS) --profile=dev

# CARGO_FLAGS_EXTRA is meant to be set outside the makefile by user
.PHONY: build
build: tarantool-patch
	if test -f ~/.cargo/env; then . ~/.cargo/env; fi && \
		$(CARGO_ENV) \
		cargo build $(LOCKED) $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) $(CARGO_FLAGS_EXTRA)

# There are 4 build options. 3 for each build profile (dev, fast-release, release).
# They are intended to be consumed by tests/local development.
# Remaining `build-release-pkg` is intended for packages we ship as our release artifacts.
# For now the only difference is absence of error_injection feature.
.PHONY: build-dev
build-dev: override CARGO_FLAGS += $(ERROR_INJECTION)
build-dev: override CARGO_FLAGS += --profile=dev
build-dev: build-plug-wrong-version
build-dev: build

.PHONY: build-fast-release
build-fast-release: override CARGO_FLAGS += $(ERROR_INJECTION)
build-fast-release: override CARGO_FLAGS += --profile=fast-release
build-fast-release: build-plug-wrong-version
build-fast-release: build

.PHONY: build-release
build-release: override CARGO_FLAGS += $(ERROR_INJECTION)
build-release: override CARGO_FLAGS += --profile=release
build-release: build-plug-wrong-version
build-release: build

# Ignore CARGO_FLAGS defaults from the above by using `=` instead of `+=`.
# We only use `override` for the mandatory flags, because:
#  - We want to give user a certain level of control over CARGO_FLAGS.
#  - At the same time, the package must always be optimized, include binaries etc.
.PHONY: build-release-pkg
build-release-pkg: CARGO_FLAGS = # reset the defaults
build-release-pkg: CARGO_FLAGS += $(if $(USE_DYNAMIC_BUILD),--features dynamic_build)
build-release-pkg: override CARGO_FLAGS += -p picodata -p gostech-audit-log --features webui
build-release-pkg: override CARGO_FLAGS += --profile=release
build-release-pkg: build

# We have to specify target to disable ASan for proc macros, build.rs, etc.
# See https://github.com/rust-lang/cargo/issues/6375#issuecomment-444900324.
DEFAULT_TARGET := $(shell cargo -vV | sed -n 's|host: ||p')

# TODO: drop nightly features once sanitizers are stable.
.PHONY: build-asan-dev
build-asan-dev: override CARGO_ENV = RUSTC_BOOTSTRAP=1 RUSTFLAGS=-Zsanitizer=address
build-asan-dev: override CARGO_FLAGS += --target=$(DEFAULT_TARGET)
build-asan-dev: override CARGO_FLAGS += --profile=asan-dev
build-asan-dev: build

# XXX: make sure we pass proper flags to cargo test so resulting picodata binary can be
# reused for python tests without recompilation
# Note: tarantool and tlua are skipped intentionally, no need to run their doc tests in picodata
# Note: non-doc tests and doc tests are run separately. This is intended to prevent excessive
# memory usage caused by doc tests compilation model. Doc tests are compiled as part of actual test run.
# So, each parallel thread lanuched by cargo test spawns full blown compiler for each doctest
# which at the end leads to OOM.
.PHONY: test-rs
test-rs:
	cargo test $(LOCKED) $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) $(CARGO_FLAGS_EXTRA) $(ERROR_INJECTION) \
	  --exclude sbroad-core \
	  --exclude tarantool \
	  --exclude tlua \
	  --tests

	cargo test $(LOCKED) $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) $(CARGO_FLAGS_EXTRA) $(ERROR_INJECTION) \
	  --exclude sbroad-core \
	  --exclude tarantool \
	  --exclude tlua \
	  --doc -- --test-threads 2

.PHONY: test-py
test-py:
	poetry run pytest $(PYTEST_NUMPROCESSES) $(PYTEST_FLAGS) -vv --color=yes

.PHONY: test
test: test-rs test-py

.PHONY: generate-snapshot
generate-snapshot:
	poetry run python3 test/generate_snapshot.py

.PHONY: lint-rs
lint-rs:
	cargo version

	cargo fmt --check

	RUSTFLAGS="-Dwarnings -Adeprecated" cargo check $(LOCKED) $(MAKE_JOBSERVER_ARGS)

	cargo clippy --version
	cargo clippy \
		$(LOCKED) $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) \
		--features=load_test,error_injection \
		--exclude tarantool \
		--exclude tarantool-proc \
		--exclude tlua \
		-- --deny clippy::all --no-deps

	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" \
		cargo doc \
			$(LOCKED) $(MAKE_JOBSERVER_ARGS) \
			--workspace --no-deps --document-private-items \
			--exclude=tlua --exclude=tarantool

.PHONY: lint-py
lint-py:
	poetry run ruff check ./test
	poetry run ruff format ./test --check --diff
	poetry run mypy ./test

.PHONY: lint
lint: lint-rs lint-py

.PHONY: fmt
fmt:
	cargo fmt
	poetry run ruff format ./test

.PHONY: audit
audit:
	cargo deny --workspace check

.PHONY: clean
clean:
	cargo clean || true
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	find . -type d -name __pycache__ | xargs -n 500 rm -rf

.PHONY: benchmark
benchmark:
	PICODATA_LOG_LEVEL=warn poetry run pytest test/manual/test_benchmark.py

.PHONY: flamegraph
flamegraph:
	PICODATA_LOG_LEVEL=warn poetry run pytest test/manual/test_benchmark.py --with-flamegraph

.PHONY: k6
k6:
	PICODATA_LOG_LEVEL=warn poetry run pytest test/manual/sql/test_sql_perf.py

.PHONY: install
install:
	mkdir -p $(DESTDIR)/usr/bin
	install -m 0755 target/*/picodata $(DESTDIR)/usr/bin/picodata
	install -m 0755 target/*/gostech-audit-log $(DESTDIR)/usr/bin/gostech-audit-log

# IMPORTANT. This rule is primarily used in CI pack stage. It repeats
# the behavior of build.rs `build_webui()`, but uses a different out_dir
# `picodata-webui/dist` instead of `target/debug/build/picodata-webui`
.PHONY: build-webui-bundle
build-webui-bundle:
	yarn --cwd webui install \
		--prefer-offline \
		--frozen-lockfile \
		--no-progress \
		--non-interactive
	yarn --cwd webui vite build \
		--outDir dist \
		--emptyOutDir

.PHONY: reset-submodules
reset-submodules:
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	git submodule update --init --recursive

.PHONY: install-cargo
install-cargo:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
		sh -s -- -y --profile default --default-toolchain 1.85

.PHONY: centos7-cmake3
centos7-cmake3:
	if [ ! -L /usr/bin/cmake3 ] ; then \
		[ -f /usr/bin/cmake ] && sudo rm /usr/bin/cmake; \
		sudo ln -s /usr/bin/cmake3 /usr/bin/cmake; \
	fi
	sudo find {/opt,/usr} -name libgomp.spec -delete

publish-picodata-plugin:
	cargo publish --dry-run -p picodata-plugin-proc-macro
	cargo publish -p picodata-plugin-proc-macro

	cargo publish --dry-run -p picodata-plugin
	cargo publish -p picodata-plugin

build_cartridge_engine:
	(cd sbroad; make build_cartridge_engine)

install_release:
	(cd sbroad; make install_release)
