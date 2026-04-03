#!/bin/bash

# This script will send junit_pytest.xml report file to the flake-tracker service (source code: https://git.picodata.io/core/flake-tracker).
# `flake-tracker` will expose counts each test has failed in protected branches as prometheus metrics, making them viewable as grafana dashboard on https://ping.picodata.io/

if [ -z "$CI" ]; then
  echo "This script should only be ran in CI environment"
  exit 1
fi

# This script relies on $FLAKE_TRACKER_TOKEN to determine whether to send reports to grafana or not.
# The idea is that $FLAKE_TRACKER_TOKEN should only be exposed to protected gitlab branches that have stabilized code, like `master` branch or minor release branches.
# This means that test failures are unlikely to be due to the code being worked on, but are flakes - faulty tests or bugs in the product causing the test to sometimes fail.
if [ ! -z "$FLAKE_TRACKER_TOKEN" ]; then
  # $CI_JOB_NAME can contain special characters (like []), so urlencode it with python
  ENCODED_JOB_NAME=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$CI_JOB_NAME")
  curl --globoff "https://flake-tracker.picodata.io/submit-report?job=$ENCODED_JOB_NAME" -H "Authorization: Bearer $FLAKE_TRACKER_TOKEN" --data-binary '@junit_pytest.xml' &&
    echo -e "\nFinished uploading test report to flake-tracker" ||
    echo -e "\nFailed uploading test report to flake-tracker"
else
  echo "FLAKE_TRACKER_TOKEN is not set, skipping uploading test report to flake-tracker"
fi
