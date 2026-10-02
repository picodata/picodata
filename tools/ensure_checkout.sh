#!/bin/bash

# Jobs with `GIT_STRATEGY: none` take the source tree from the `cache_git`
# cache. It is keyed by ref, not by pipeline, so a newer pipeline on the same
# ref may have overwritten it before this job started (e.g. this job is a retry
# that runs after a newer push). In that case check out the commit this
# pipeline was triggered for.

set -euxo pipefail

ACTUAL_COMMIT_SHA="$(git rev-parse HEAD)"

if [[ "$CI_COMMIT_SHA" != "$ACTUAL_COMMIT_SHA" ]]; then
    echo "Cached checkout is at $ACTUAL_COMMIT_SHA, but the pipeline was triggered for $CI_COMMIT_SHA, checking it out"

    if ! git cat-file -e "$CI_COMMIT_SHA^{commit}" 2>/dev/null; then
        git fetch --no-recurse-submodules origin "$CI_COMMIT_SHA"
    fi
    git checkout --force "$CI_COMMIT_SHA"

    # Same as in clone.sh: if submodule update fails (stale ref from
    # force-push), wipe cached submodule data and retry.
    git submodule sync --recursive
    if ! git submodule update --init --recursive; then
        git submodule deinit --all --force
        rm -rf .git/modules/*
        git submodule update --init --recursive
    fi

    ACTUAL_COMMIT_SHA="$(git rev-parse HEAD)"
fi

if [[ "$CI_COMMIT_SHA" != "$ACTUAL_COMMIT_SHA" ]]; then
    echo "Git checkout messed up. Checked out version is different from the commit SHA in pipeline trigger"
    exit 1
fi
