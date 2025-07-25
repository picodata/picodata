#!/bin/bash

set -euxo pipefail

git config --global fetch.parallel 8

# check if .git exist, fetch current commit and update submodules
if [ -d "$CI_PROJECT_DIR/.git" ]; then
  cd $CI_PROJECT_DIR
  # parallel clone/fetch
  git clean -fdx

  # remove cached version of local branch/tag
  git branch -D $CI_COMMIT_REF_NAME || echo "branch $CI_COMMIT_REF_NAME doesn't exist"
  git tag -d $CI_COMMIT_REF_NAME || echo "tag $CI_COMMIT_REF_NAME doesn't exist"

  # fetch remote tags overwriting local ones (in case tag was removed from remote but still exists in cache)
  git fetch --tags --force

  git fetch origin $CI_COMMIT_REF_NAME
  git checkout $CI_COMMIT_REF_NAME --force
  git submodule update --init --recursive
else
  # do full clone
  git clone --recursive -b $CI_COMMIT_REF_NAME https://git.picodata.io/${CI_PROJECT_PATH}.git $CI_PROJECT_DIR
fi

# report what we ended up with
git rev-parse HEAD
