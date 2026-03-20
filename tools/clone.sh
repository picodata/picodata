#!/bin/bash

set -euxo pipefail

# parallel clone/fetch
git config --global fetch.parallel 8

# check if .git exist, fetch current commit and update submodules
if [ -d "$CI_PROJECT_DIR/.git" ]; then
  cd $CI_PROJECT_DIR

  git rev-parse --abbrev-ref HEAD

  git clean -fdx

  # fetch remote tags overwriting local ones
  # important in case tag was removed from remote but still exists in cache
  # (typical case when release didnt go smoothly and we want to change release tag)
  #
  # --no-recurse-submodules: submodule refs may point to commits that were
  # force-pushed away; fetching them here would fail with "not our ref".
  # Submodules are handled below by `git submodule update`.
  git fetch --tags --force --no-recurse-submodules

  # fetch fresh version of branch/tag
  git fetch origin --no-recurse-submodules $CI_COMMIT_REF_NAME

  # do checkout by commit, this works for both, regular branches and tags
  # note that there can be some weird cases, i e when cached git directory
  # was created from a branch and then pipeline triggered for force-pushed
  # version of that branch, so tip of the branch from the cache is no longer valid
  git checkout $CI_COMMIT_SHA --force

  # If submodule update fails (stale ref from force-push), wipe cached
  # submodule data and retry.
  git submodule sync --recursive
  if ! git submodule update --init --recursive; then
    git submodule deinit --all --force
    rm -rf .git/modules/*
    git submodule update --init --recursive
  fi
else
  # do full clone
  git clone --recursive -b $CI_COMMIT_REF_NAME https://git.picodata.io/${CI_PROJECT_PATH}.git $CI_PROJECT_DIR
fi

# report what we ended up with
git rev-parse HEAD
