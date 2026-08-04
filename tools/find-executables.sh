#!/usr/bin/env bash

set -euo pipefail

DIR=$(realpath "${1:-$PWD}")
{
  find "$DIR" -executable -type f -exec file -i {} + \
    | awk -F: '/application\/x-(pie-)?(executable|sharedlib)/ { print $1 }'
  find "$DIR" -executable -type f -name "*.so"
} | sort -u
