#!/usr/bin/env bash

set -euo pipefail

DIR=$(realpath ${1:-"$PWD"})
find "$DIR" -executable -type f -exec file -i {} \; | grep "application/x-executable" | cut -f1 -d':'
find "$DIR" -executable -type f -name "*.so"
