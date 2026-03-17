#!/bin/bash

set -euxo pipefail

PROJECT_DIR=$(dirname "$(dirname "$(realpath "$0")")")
SBOM_METADATA_DIR=$PROJECT_DIR/certification/sbom/metadata
SBOM_CHECKER_DIR=${SBOM_CHECKER_DIR:-$PROJECT_DIR/certification/sbom/sbom-checker}
SBOM_VERIFY=${SBOM_VERIFY:-1}
SBOM_RUST=${SBOM_RUST:-1}
SBOM_JS=${SBOM_JS:-1}


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

if [ "$SBOM_JS" == "1" ]; then
    # build separate SBOM file for webui
    # see "sbom" script in package.json
    # we use a full
    pushd webui
    yarn install --immutable
    yarn cyclonedx --prod --gather-license-texts --output-reproducible -o ../webui.cdx.json
    popd

    # We do not pass attack_surface and security_function files because for webui there are no
    # security sensitive components, so script sets default "no" value for all components
    python3 "$SBOM_CHECKER_DIR"/picodata_sbom_tools.py \
        --manufacturer "ООО Пикодата" \
        "$PROJECT_DIR"/webui.cdx.json "$PROJECT_DIR"/webui-fixed.cdx.json
fi


if [ "$SBOM_RUST" != "1" ]; then
    # all the lines below are rust sbom building
    exit 0
fi

# Needed for CI when toolchain is installed and immediatly used in the same shell session
if test -f "$HOME"/.cargo/env; then . "$HOME"/.cargo/env; fi

cargo install --quiet --locked \
    --git https://github.com/evanmiller2112/cyclonedx-rust-cargo \
    cargo-cyclonedx

cargo cyclonedx --format json --spec-version 1.6


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
    python3 "$SBOM_CHECKER_DIR"/sbom-checker.py --check-vcs -v "$PROJECT_DIR"/picodata-fixed.cdx.json
    python3 "$SBOM_CHECKER_DIR"/sbom-checker.py --check-source-distribution -v "$PROJECT_DIR"/picodata-fixed.cdx.json
    python3 "$SBOM_CHECKER_DIR"/sbom-checker.py --check-vcs -v "$PROJECT_DIR"/webui-fixed.cdx.json
    python3 "$SBOM_CHECKER_DIR"/sbom-checker.py --check-source-distribution -v "$PROJECT_DIR"/webui-fixed.cdx.json
fi
