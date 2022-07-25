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

TARANTOOL_EXECUTABLE=${TARANTOOL_EXECUTABLE:-tarantool}

WORKSPACE_ROOT=$(
    cargo metadata --format-version=1 |
        $TARANTOOL_EXECUTABLE -e \
            'print(require("json").decode(io.read("*l")).workspace_root)'
)

"${TARANTOOL_EXECUTABLE}" "${WORKSPACE_ROOT}/tests/run_tests.lua" "${FILTER}"
