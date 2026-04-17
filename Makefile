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

PYTEST_FLAGS :=

# Devs may want to drop this flag using `make LOCKED=`.
LOCKED := --locked
# Sometimes we may not want to build the whole workspace.
# For instance, we've seen some problems with `cargo build --workspace --tests`.
WORKSPACE := --workspace

# Give a chance to tune features without overriding the whole CARGO_FLAGS.
WEBUI := --features=webui
ERROR_INJECTION := --features=error_injection
CARGO_FEATURES := $(ERROR_INJECTION) $(WEBUI)

# XXX: It should be possible to keep the flags to the bare minimum.
# Hence, we don't use `override` here but add it to all build prerequisites.
CARGO_FLAGS := $(LOCKED) $(WORKSPACE) $(CARGO_FEATURES)

# XXX: Specialized CARGO_TARGET_DIR values for ASan & code coverage.
# We don't want to overwrite a regular build with these.
TARGET_DIR_ASAN := target/asan-dev
TARGET_DIR_COV := target/cov

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

# Both CARGO_FLAGS and CARGO_FLAGS_EXTRA can be passed externally:
#  - CARGO_FLAGS overrides whatever we set internally
#  - CARGO_FLAGS_EXTRA only appends to it without overriding
.PHONY: build
build: tarantool-patch
	if test -f ~/.cargo/env; then . ~/.cargo/env; fi && \
		$(CARGO_ENV) \
		cargo build $(MAKE_JOBSERVER_ARGS) $(CARGO_FLAGS) $(CARGO_FLAGS_EXTRA)

# There are 4 build options. 3 for each build profile (dev, fast-release, release).
# They are intended to be consumed by tests/local development.
# Remaining `build-release-pkg` is intended for packages we ship as our release artifacts.
# For now the only difference is absence of error_injection feature.
.PHONY: build-dev
build-dev: override CARGO_FLAGS += $(ERROR_INJECTION)
build-dev: override CARGO_FLAGS += --profile=dev
build-dev: build
build-dev: build-plug-wrong-version

.PHONY: build-fast-release
build-fast-release: override CARGO_FLAGS += $(ERROR_INJECTION)
build-fast-release: override CARGO_FLAGS += --profile=fast-release
build-fast-release: build
build-fast-release: build-plug-wrong-version

.PHONY: build-release
build-release: override CARGO_FLAGS += $(ERROR_INJECTION)
build-release: override CARGO_FLAGS += --profile=release
build-release: build
build-release: build-plug-wrong-version

# Ignore CARGO_FLAGS defaults from the above by using `=` instead of `+=`.
# We only use `override` for the mandatory flags, because:
#  - We want to give user a certain level of control over CARGO_FLAGS.
#  - At the same time, the package must always be optimized, include binaries etc.
.PHONY: build-release-pkg
build-release-pkg: CARGO_FLAGS = # reset the defaults
build-release-pkg: CARGO_FLAGS += $(if $(USE_DYNAMIC_BUILD),--features dynamic_build)
build-release-pkg: override CARGO_FLAGS += -p picodata --features webui
build-release-pkg: override CARGO_FLAGS += --profile=release
build-release-pkg: override CARGO_FLAGS += $(LOCKED)
build-release-pkg: build

# We have to specify target to disable ASan for proc macros, build.rs, etc.
# See https://github.com/rust-lang/cargo/issues/6375#issuecomment-444900324.
DEFAULT_TARGET := $(shell cargo -vV | sed -n 's|host: ||p')

# ASan build: uses --cfg asan flag instead of a dedicated Cargo profile.
# Artifacts go to asan-dev/ to avoid conflicts with regular builds.
# TODO: drop nightly features once sanitizers are stable.
.PHONY: build-asan-dev
build-asan-dev: override CARGO_ENV = RUSTC_BOOTSTRAP=1
build-asan-dev: override CARGO_ENV += CARGO_TARGET_DIR=$(TARGET_DIR_ASAN)
build-asan-dev: override CARGO_ENV += RUSTFLAGS='-Zsanitizer=address --cfg asan'
build-asan-dev: override CARGO_ENV += RUSTDOCFLAGS='-Zsanitizer=address --cfg asan'
build-asan-dev: override CARGO_FLAGS += --target=$(DEFAULT_TARGET)
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
	cargo test \
	  $(MAKE_JOBSERVER_ARGS) \
	  $(filter-out --workspace, $(CARGO_FLAGS)) \
	  $(filter-out --workspace, $(CARGO_FLAGS_EXTRA)) \
	  --workspace \
	  --exclude sql-planner \
	  --exclude tarantool \
	  --exclude tlua \
	  --tests

	cargo test \
	  $(MAKE_JOBSERVER_ARGS) \
	  $(filter-out --workspace, $(CARGO_FLAGS)) \
	  $(filter-out --workspace, $(CARGO_FLAGS_EXTRA)) \
	  --workspace \
	  --exclude sql-planner \
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

	RUSTFLAGS="-Dwarnings -Adeprecated" \
	  cargo check \
	    $(MAKE_JOBSERVER_ARGS) \
	    $(filter-out --workspace, $(CARGO_FLAGS)) \
	    $(filter-out --workspace, $(CARGO_FLAGS_EXTRA)) \
	    --workspace \
	    --benches \
	    --tests

	cargo clippy --version
	cargo clippy \
	  $(MAKE_JOBSERVER_ARGS) \
	  $(filter-out --workspace, $(CARGO_FLAGS)) \
	  $(filter-out --workspace, $(CARGO_FLAGS_EXTRA)) \
	  --workspace \
	  --features=load_test,error_injection,demo \
	  -- --deny clippy::all --no-deps

	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" \
	  cargo doc \
	    $(MAKE_JOBSERVER_ARGS) \
	    $(filter-out --workspace, $(CARGO_FLAGS)) \
	    $(filter-out --workspace, $(CARGO_FLAGS_EXTRA)) \
	    --workspace \
	    --document-private-items \
	    --no-deps

.PHONY: lint-py
lint-py:
	poetry run ruff check ./test
	poetry run ruff format ./test tools benchmark --check --diff
	poetry run mypy ./test

.PHONY: lint
lint: lint-rs lint-py

.PHONY: fmt
fmt:
	cargo fmt
	poetry run ruff format ./test tools benchmark

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


.PHONY: coverage-test-rs
coverage-test-rs: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-test-rs:
	tools/coverage.py run $(MAKE) test-rs

.PHONY: coverage-test-py
coverage-test-py: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-test-py:
	tools/coverage.py run $(MAKE) test-py

.PHONY: coverage-build
coverage-build: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-build:
	# Build everything in advance.
	tools/coverage.py run $(MAKE) build CARGO_FLAGS_EXTRA="--lib --bins --tests"
	tools/coverage.py run $(MAKE) build-plug-wrong-version

.PHONY: coverage-report
coverage-report: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-report:
	tools/find-executables.sh $(CARGO_TARGET_DIR) > $(CARGO_TARGET_DIR)/binaries
	tools/coverage.py report --input-objects=$(CARGO_TARGET_DIR)/binaries --open

.PHONY: coverage-clean
coverage-clean: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-clean:
	tools/coverage.py clean

.PHONY: coverage-purge
coverage-purge: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-purge:
	rm -rf $(CARGO_TARGET_DIR)

# XXX: this target is for debug purposes (do not use in CI).
.PHONY: coverage-demo
coverage-demo: export CARGO_TARGET_DIR=$(TARGET_DIR_COV)
coverage-demo:
	$(MAKE) coverage-build

	# Drop any possible coverage data for `build.rs`.
	$(MAKE) coverage-clean

	# Note that it's better to first run rust-based tests,
	# then the python-based ones to prevent coverage loss
	# due to accidental rebuilds changing signatures of bins.
	$(MAKE) coverage-test-rs
	$(MAKE) coverage-test-py

	$(MAKE) coverage-report


.PHONY: k6
k6:
	PICODATA_LOG_LEVEL=warn poetry run pytest test/manual/sql/test_sql_perf.py

.PHONY: install
install:
	mkdir -p $(DESTDIR)/usr/bin
	install -m 0755 target/*/picodata $(DESTDIR)/usr/bin/picodata

# XXX: This rule is primarily used in CI pack stage.
.PHONY: build-webui-bundle
build-webui-bundle:
	$(MAKE) -C webui build

.PHONY: reset-submodules
reset-submodules:
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	git submodule update --init --recursive

.PHONY: install-cargo
install-cargo:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
		sh -s -- -y --profile default --default-toolchain 1.89

.PHONY: centos7-cmake3
centos7-cmake3:
	if [ ! -L /usr/bin/cmake3 ] ; then \
		[ -f /usr/bin/cmake ] && sudo rm /usr/bin/cmake; \
		sudo ln -s /usr/bin/cmake3 /usr/bin/cmake; \
	fi
	sudo find {/opt,/usr} -name libgomp.spec -delete

.PHONY: publish-picodata-plugin
publish-picodata-plugin:
	cargo publish --dry-run -p picodata-plugin-proc-macro
	cargo publish -p picodata-plugin-proc-macro

	cargo publish --dry-run -p picodata-plugin
	cargo publish -p picodata-plugin


.PHONY: sbom-rust
sbom-rust:
	SBOM_JS=0 ./tools/sbom.sh

.PHONY: sbom-js
sbom-js:
	SBOM_RUST=0 ./tools/sbom.sh

.PHONY: sbom
sbom:
	./tools/sbom.sh


# IMPORTANT. This rule should only be executed via CI.
# It updates the pinned message in the `pico-dev` chat.
.PHONY: flaky-finder
flaky-finder:
	echo "Looking for flaky tests on master..."
	echo "*Flaky tests on master ($(DAYS) days)*" > report.txt
	echo >> report.txt
	set +o pipefail && ./tools/flaky_finder.py --days $(DAYS) --csv-format | \
		while read -r row; do awk -F, '{printf "`%s` -> %s ([pipeline](%s))\n", $$1, $$3, $$2}'; done >> report.txt
	echo >> report.txt
	echo "Edited on _$(shell date)_ by [job]($(CI_JOB_URL))." >> report.txt
	echo "Sending report update via bot..."
	curl "https://api.telegram.org/bot${FLAKY_INFORMER_BOT_TOKEN}/editMessageText?chat_id=$(TG_CHAT_ID)&message_id=$(TG_MESSAGE_ID)&parse_mode=Markdown" \
		--data-urlencode "text@report.txt" --data-urlencode "link_preview_options={\"url\":\"$(CI_JOB_URL)\"}"

.PHONY: collect-versions
collect-required-rolling-versions:
	poetry run pytest --collect-only --collect-required-rolling-versions
