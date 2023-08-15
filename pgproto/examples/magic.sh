#!/usr/bin/env bash
set -eux
cargo build && rm -rf -- *.snap *.xlog && examples/demo.lua
