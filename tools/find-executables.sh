#!/usr/bin/env bash

set -euo pipefail

DIR=$(realpath "${1:-$PWD}")
# `file -i` reports position-independent binaries as `application/x-pie-executable`,
# which is what rustc emits for library-crate test binaries (the ones that link
# tarantool come out as plain `application/x-executable`).
# awk rather than `grep | cut`, so that finding nothing is not a pipefail exit.
find "$DIR" -executable -type f -exec file -i {} + \
  | awk -F: '/application\/x-(pie-)?executable/ { print $1 }'
find "$DIR" -executable -type f -name "*.so"
