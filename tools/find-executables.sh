#!/usr/bin/env bash

set -euo pipefail

DIR=$(realpath "${1:-$PWD}")
# Skip tarantool's own tests (built due to BUILD_TESTING), they take gigabytes.
PRUNE=(-path '*/build/tarantool-sys/*/test' -prune -o)
{
  find "$DIR" "${PRUNE[@]}" -executable -type f -exec file -i {} + \
    | awk -F: '/application\/x-(pie-)?(executable|sharedlib)/ { print $1 }'
  find "$DIR" "${PRUNE[@]}" -executable -type f -name "*.so" -print
} | sort -u
