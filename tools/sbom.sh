#!/bin/bash

set -euxo pipefail

PROJECT_DIR=$(dirname "$(dirname "$(realpath "$0")")")
SBOM_METADATA_DIR=$PROJECT_DIR/certification/sbom/metadata
SBOM_CHECKER_DIR=${SBOM_CHECKER_DIR:-$PROJECT_DIR/certification/sbom/sbom-checker}
SBOM_VERIFY=${SBOM_VERIFY:-1}

# Needed for CI when toolchain is installed and immediatly used in the same shell session
if test -f "$HOME"/.cargo/env; then . "$HOME"/.cargo/env; fi

cargo install --quiet --locked \
    --git https://github.com/evanmiller2112/cyclonedx-rust-cargo \
    cargo-cyclonedx

cargo cyclonedx --format json --spec-version 1.6

# build separate SBOM file for webui
# see "sbom" script in package.json
# we use a full
pushd webui
yarn install --immutable
yarn cyclonedx --prod --gather-license-texts --output-reproducible -o ../webui_sbom.json
popd

# Not a submodule because we have github mirror, so we would have to mirror this repo there as well for
# --recurse-submodules to work during git clone
if [ ! -d "$SBOM_CHECKER_DIR" ]; then
    git clone git@git.picodata.io:core/sbom-checker.git "$SBOM_CHECKER_DIR"
    if [ -z "${VIRTUAL_ENV:-}" ]; then
        python3 -m venv "$SBOM_CHECKER_DIR/venv"
        . "$SBOM_CHECKER_DIR/venv/bin/activate"
        pip install -r "$SBOM_CHECKER_DIR/requirements.txt"
    fi
fi

python3 "$SBOM_CHECKER_DIR"/picodata_sbom_tools.py \
    --manufacturer "ООО Пикодата" \
    --external-references-map-file "$SBOM_METADATA_DIR"/external_references.json \
    --attack-surface-map-file "$SBOM_METADATA_DIR"/attack_surface.json \
    --security-function-map-file "$SBOM_METADATA_DIR"/security_function.json \
    --extra-components "$SBOM_METADATA_DIR"/extra_components.json \
    "$PROJECT_DIR"/picodata.cdx.json "$PROJECT_DIR"/picodata-fixed.cdx.json

# sbom-checker requires python >= 3.8. not all our supported OS have such version
if [ "$SBOM_VERIFY" == "1" ]; then
    if [ -z "${VIRTUAL_ENV:-}" -a -f "$SBOM_CHECKER_DIR/venv/bin/activate" ]; then
    . "$SBOM_CHECKER_DIR/venv/bin/activate"
    fi
    python3 "$SBOM_CHECKER_DIR"/sbom-checker.py -v "$PROJECT_DIR"/picodata-fixed.cdx.json
fi
