#!/bin/bash

set -euxo pipefail

ACTUAL_COMMIT_SHA="$(git rev-parse HEAD)"

if [[ "$CI_COMMIT_SHA" != "$ACTUAL_COMMIT_SHA" ]]; then
    echo "Git checkout messed up. Checked out version is different from the commit SHA in pipeline trigger"
    exit 1
fi
