#!/bin/bash

# NOTE(kbezuglyi):
# 
# * This script is used for tests on rolling upgrades. It downloads needed
#   executables to our Docker image for the CI.
# 
# * We only need previous minor and major versions, as well as minor and major
#   versions before previous ones.
# 
# * It is not possible to use only `dnf`/`rpm`/`yum` with our repository, as these
#   package managers does not allow to install two different versions of the same
#   software alongside together.
#
# * Even though these executables are quite large, as long as we cache our built
#   Docker images throughout the CI, it is a considerable trade-off.

set -euxo pipefail

function install {
  local version="$1"
  local tmpdir="/tmp/picodata-$version"

  mkdir -p "$tmpdir"
  pushd "$tmpdir"

  dnf download "picodata-$version.*"

  local rpmfile=$(ls picodata-"$version".*.rpm)
  # NOTE(kbezuglyi):
  # When we transit to the EL9 (e.g., Rocky Linux 9), do not forget to
  # consider [this bug](https://bugzilla.redhat.com/show_bug.cgi?id=2058426#c3).
  rpm2cpio "$rpmfile" | cpio -idmv

  local pattern='^picodata-([0-9]+\.[0-9]+\.[0-9]+).*'
  local extracted=$(echo "$rpmfile" | sed -E "s/${pattern}/\1/")
  mv ./usr/bin/picodata "/usr/local/bin/picodata-$extracted"

  popd
  rm -rf "$tmpdir"
}

install "25.2"
install "25.3"
