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
CARGO_FLAGS := --features webui

# Devs may want to drop this flag using make LOCKED=
LOCKED := --locked

.PHONY: default
default: ;

.PHONY: tarantool-patch
tarantool-patch:
	echo "${VER_TNT}" > tarantool-sys/VERSION

# CARGO_FLAGS_EXTRA is meant to be set outside the makefile by user
.PHONY: build
build: tarantool-patch
	if test -f ~/.cargo/env; then . ~/.cargo/env; fi && $(CARGO_ENV) \
		cargo build $(LOCKED) $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) $(CARGO_FLAGS_EXTRA)

.PHONY: build-dev
build-dev: override CARGO_FLAGS += --profile=dev
build-dev: build

.PHONY: build-release
build-release: override CARGO_FLAGS += --profile=release
build-release: build

.PHONY: test
test:
	cargo test $(LOCKED) $(MAKE_JOBSERVER_ARGS) $(PROFILE)
	pipenv run pytest $(PYTEST_NUMPROCESSES)

.PHONY: lint
lint:
	cargo fmt --check

	cargo check $(LOCKED) $(MAKE_JOBSERVER_ARGS)

	cargo clippy --version
	cargo clippy \
		$(LOCKED) $(MAKE_JOBSERVER_ARGS) \
		--features=load_test,error_injection,webui \
		-- --deny clippy::all --no-deps

	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" \
		cargo doc \
			$(LOCKED) $(MAKE_JOBSERVER_ARGS) \
			--workspace --no-deps --document-private-items \
			--exclude=tlua --exclude=sbroad-core --exclude=tarantool

	pipenv run lint

.PHONY: fmt
fmt:
	cargo fmt
	pipenv run fmt

.PHONY: clean
clean:
	cargo clean || true
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	find . -type d -name __pycache__ | xargs -n 500 rm -rf

.PHONY: benchmark
benchmark:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/test_benchmark.py

.PHONY: flamegraph
flamegraph:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/test_benchmark.py --with-flamegraph

.PHONY: k6
k6:
	PICODATA_LOG_LEVEL=warn pipenv run pytest test/manual/sql/test_sql_perf.py

.PHONY: install
install:
	mkdir -p $(DESTDIR)/usr/bin
	install -m 0755 target/*/picodata $(DESTDIR)/usr/bin/picodata

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
		sh -s -- -y --profile default --default-toolchain 1.76.0

.PHONY: centos7-cmake3
centos7-cmake3:
	if [ ! -L /usr/bin/cmake3 ] ; then \
		[ -f /usr/bin/cmake ] && sudo rm /usr/bin/cmake; \
		sudo ln -s /usr/bin/cmake3 /usr/bin/cmake; \
	fi
	sudo find {/opt,/usr} -name libgomp.spec -delete
