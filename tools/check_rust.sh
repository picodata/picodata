#!/bin/sh

# Check rust version consistency
exec 3>&1 # duplicate stdout fd
grep_rust_version() { echo "$1: $(sed -nr "s/.*rust-version = \"(\S+)\".*/\1/p" $1)"; }
grep_toolchain() { echo "$1: $(sed -nr "s/.*--default-toolchain (\S+).*/\1/p" $1)"; }
UNIQUE_VERSIONS=$(
  {
    grep_rust_version Cargo.toml;
    grep_toolchain Makefile;
  } \
  | tee /dev/fd/3 \
  | cut -d: -f2- | sort | uniq | wc -l
);

if [ "$UNIQUE_VERSIONS" != "1" ]; then
  echo "Error: checking rust version consistency failed"
  exit 1
fi
