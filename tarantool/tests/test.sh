#!/usr/bin/bash

set -e

FILTER="$1"

WORKSPACE_ROOT=$(
    cargo metadata --format-version=1 |
        tarantool -e \
            'print(require("json").decode(io.read("*l")).workspace_root)'
)

cargo build -p tarantool-module-test-runner

"${WORKSPACE_ROOT}/tests/run_tests.lua" "${FILTER}"
