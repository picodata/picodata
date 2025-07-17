#!/bin/bash

set -euxo pipefail

# get start time
start_time=$(date +%s)

# check if .git exist, fetch current commit and update submodules
if [ -d "$CI_PROJECT_DIR/.git" ]; then
  cd $CI_PROJECT_DIR
  # parallel clone/fetch
  git config fetch.parallel 8
  git clean -fdx
  git branch -D $CI_COMMIT_REF_NAME || echo "$CI_COMMIT_REF_NAME doesn't exist"
  git fetch -uf origin $CI_COMMIT_REF_NAME:$CI_COMMIT_REF_NAME
  git checkout $CI_COMMIT_REF_NAME --force
  git submodule update --init --recursive
else
  # do full clone
  git clone --recursive -b $CI_COMMIT_REF_NAME -j8 https://git.picodata.io/${CI_PROJECT_PATH}.git $CI_PROJECT_DIR
fi

# get end time and difference
date
end_time=$(date +%s)
difference=$((end_time - start_time))
echo "Clone took: $difference seconds"
