#!/bin/sh
# Call this script to install test dependencies

set -e

# Test dependencies:
tarantoolctl rocks install luatest 0.5.0
tarantoolctl rocks install luacov 0.13.0
tarantoolctl rocks install luacheck 0.25.0
