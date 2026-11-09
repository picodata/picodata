#!/bin/bash

set -euo pipefail

: "${RUST_VERSION:?RUST_VERSION must be set}"
: "${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}"

cargo +$RUST_VERSION build --locked --timings
cp "$CARGO_TARGET_DIR/cargo-timings/cargo-timing.html" cargo-timing-without-webui.html
cargo +$RUST_VERSION build --features webui --locked --timings
cp "$CARGO_TARGET_DIR/cargo-timings/cargo-timing.html" cargo-timing-with-webui.html
cp "$CARGO_TARGET_DIR/debug/picodata" picodata
# There are no Rust tests for `webui` feature.
# It will be checked during integration tests.
cargo +$RUST_VERSION test --locked

cargo +$RUST_VERSION fmt -- -v --check
cargo +$RUST_VERSION clippy --version
cargo +$RUST_VERSION clippy --features "load_test webui error_injection" -- --deny clippy::all --no-deps
