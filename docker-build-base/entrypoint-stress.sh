#!/bin/bash

export TARGET_ROOT=target/
test=${STRESS_TEST}

echo "test=$test"
if [ "$test" == "" ]; then
  echo "Use STRESS_TEST variable for set test value"
  exit 1
fi

make stress_start && \
make stress_init test=${test} && \
touch /etc/OK && \
sleep infinity
