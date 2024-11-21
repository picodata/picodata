#!/bin/sh

# Call this script to install test dependencies.

set -e

tarantoolctl rocks install luacheck 0.25.0
