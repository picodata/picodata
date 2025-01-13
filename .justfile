#****************#
# settings: GLOB #
#****************#

set quiet

[private]
default:
	{{ JUST_EXEC }} --list --unsorted --no-aliases


#***************#
# settings: ENV #
#***************#

JUST_EXEC := just_executable()
PYTEST_JOBS := "--numprocesses=auto"
CARGO_FLAGS_EXTRA := env("CARGO_FLAGS_EXTRA", "")
CARGO_FLAGS := "--features webui"
CARGO_PROFILE := env("CARGO_PROFILE", "")
CARGO_ENV := env("CARGO_ENV", "")
LOCKED := "--locked"
LOG_WARN := "PICODATA_LOG_LEVEL=warn"

#**************#
# group: BUILD #
#**************#

alias b := build
[group("build")]
[doc("`b`: build project")]
build *ARGS:
	if test -f ~/.cargo/env; then . ~/.cargo/env; fi && {{ CARGO_ENV }} \
		cargo build {{ LOCKED }} {{ ARGS }} {{ CARGO_PROFILE }} {{ CARGO_FLAGS }} {{ CARGO_FLAGS_EXTRA }}

alias bd := build-dev
[group("build")]
[doc("`bd`: development profile")]
build-dev *ARGS:
	{{ JUST_EXEC }} build {{ ARGS }} --profile=dev

alias br := build-release
[group("build")]
[doc("`br`: release profile")]
build-release *ARGS:
	{{ JUST_EXEC }} build {{ ARGS }} --profile=release

alias bw := build-webui
[group("build")]
[doc("`bw`: webui bundle")]
build-webui:
	yarn --cwd webui install \
		--prefer-offline \
		--frozen-lockfile \
		--no-progress \
		--non-interactive
	yarn --cwd webui vite build \
		--outDir dist \
		--emptyOutDir

#*************#
# group: TEST #
#*************#

alias t := test
[group("test")]
[doc("`t`: integration and unit tests")]
test *ARGS:
	{{ JUST_EXEC }} test-rust {{ ARGS }}
	{{ JUST_EXEC }} test-python

alias tr := test-rust
[group("test")]
[doc("`tr`: rust tests")]
test-rust *ARGS:
	cargo test {{ LOCKED }} {{ ARGS }} {{ CARGO_PROFILE }}

alias tp := test-python
[group("test")]
[doc("`tp`: python tests")]
test-python:
	poetry run pytest {{ PYTEST_JOBS }}

#*************#
# group: LINT #
#*************#

alias l := lint
[group("lint")]
[doc("`l`: checks and lints")]
lint *ARGS:
	{{ JUST_EXEC }} lint-rust {{ ARGS }}
	{{ JUST_EXEC }} lint-python

alias lr := lint-rust
[group("lint")]
[doc("`lr`: rust lints")]
lint-rust *ARGS:
	cargo fmt --check
	cargo check {{ LOCKED }} {{ ARGS }}
	cargo clippy --version
	cargo clippy {{ LOCKED }} {{ ARGS }} {{ CARGO_FLAGS }} \
	--features=load_test,error_injection -- --deny clippy::all --no-deps
	RUSTDOCFLAGS="-Dwarnings -Arustdoc::private_intra_doc_links" \
		cargo doc {{ LOCKED }} {{ ARGS }} \
		--workspace --no-deps --document-private-items \
		--exclude=tlua --exclude=sbroad-core --exclude=tarantool

alias lp := lint-python
[group("lint")]
[doc("`lp`: python lints")]
lint-python:
	poetry run flake8 ./test
	poetry run black ./test --check --diff
	poetry run mypy ./test

#************#
# group: FMT #
#************#

alias f := fmt
[group("fmt")]
[doc("`f`: format the code")]
fmt:
	{{ JUST_EXEC }} fmt-rust
	{{ JUST_EXEC }} fmt-python

alias fr := fmt-rust
[group("fmt")]
[doc("`fr`: rust format")]
fmt-rust:
	cargo fmt

alias fp := fmt-python
[group("fmt")]
[doc("`fp`: python format")]
fmt-python:
	poetry run black ./test

#**************#
# group: STATS #
#**************#

alias sf := flamegraph
[group("stats")]
[doc("`sf`: flamegraph for benchmark recipe")]
flamegraph:
	{{ LOG_WARN }} poetry run pytest test/manual/test_benchmark.py --with-flamegraph

alias sb := benchmark
[group("stats")]
[doc("`sb`: integration sql benchmarks")]
benchmark:
	{{ LOG_WARN }} poetry run pytest test/manual/test_benchmark.py

alias sk := k6
[group("stats")]
[doc("`sk`: sql benchmarks with k6")]
k6:
	{{ LOG_WARN }} poetry run pytest test/manual/sql/test_sql_perf.py

#*************#
# group: MISC #
#*************#

[group("misc")]
[doc("remove cargo and python cache, clean submodules")]
[confirm("Do you really want to clear all cache and submodules? (y/n)")]
clean:
	cargo clean || true
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	find . -type d -name __pycache__ | xargs -n 500 rm -rf

[group("misc")]
[doc("install binaries and documentation to DEST")]
install DEST:
	mkdir -p {{ DEST }}/usr/bin
	install -m 0755 target/*/picodata {{ DEST }}/usr/bin/picodata

[group("misc")]
[doc("install appropriate rust toolchain version")]
toolchain:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
		sh -s -- -y --profile default --default-toolchain 1.76.0

[group("misc")]
[doc("reset git submodules to initial state and update")]
reset-submodules:
	git submodule foreach --recursive 'git clean -dxf && git reset --hard'
	git submodule update --init --recursive

[group("misc")]
[doc("patch tarantool version")]
tarantool-patch:
	@echo "${VER_TNT}" > tarantool-sys/VERSION
