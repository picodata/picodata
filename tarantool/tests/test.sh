#!/usr/bin/env bash

set -e

case "$1" in
    -t | --tarantool )
        TARANTOOL_EXECUTABLE="$2"
        shift
        shift
        ;;
esac

FILTER="$1"

WORKSPACE_ROOT=$(
    cargo metadata --format-version=1 |
        tarantool -e \
            'print(require("json").decode(io.read("*l")).workspace_root)'
)

TARANTOOL_EXECUTABLE=${TARANTOOL_EXECUTABLE:-tarantool}
"${TARANTOOL_EXECUTABLE}" "${WORKSPACE_ROOT}/tests/run_tests.lua" "${FILTER}"
