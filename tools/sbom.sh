#!/bin/bash

set -euxo pipefail

PROJECT_DIR=$(dirname "$(dirname "$(realpath "$0")")")
SBOM_METADATA_DIR=$PROJECT_DIR/certification/sbom/metadata
SBOM_CHECKER_DIR=$PROJECT_DIR/certification/sbom/sbom-checker

# Needed for CI when toolchain is installed and immediatly used in the same shell session
if test -f "$HOME"/.cargo/env; then . "$HOME"/.cargo/env; fi

# Not a submodule because we have github mirror, so we would have to mirror this repo there as well for
# --recurse-submodules to work during git clone
if [ ! -d "certification/sbom/sbom-checker" ]; then
    git clone git@git.picodata.io:core/sbom-checker.git "$SBOM_CHECKER_DIR"
fi

cargo install --quiet --locked \
    --git https://github.com/evanmiller2112/cyclonedx-rust-cargo \
    cargo-cyclonedx

cargo cyclonedx --format json --spec-version 1.6

python "$SBOM_CHECKER_DIR"/picodata_sbom_tools.py \
    --manufacturer "ООО Пикодата" \
    --external-references-map-file "$SBOM_METADATA_DIR"/external_references.json \
    --attack-surface-map-file "$SBOM_METADATA_DIR"/attack_surface.json \
    --security-function-map-file "$SBOM_METADATA_DIR"/security_function.json \
    --extra-components "$SBOM_METADATA_DIR"/extra_components.json \
    "$PROJECT_DIR"/picodata.cdx.json "$PROJECT_DIR"/picodata-fixed.cdx.json

python "$SBOM_CHECKER_DIR"/sbom-checker.py -v "$PROJECT_DIR"/picodata-fixed.cdx.json
