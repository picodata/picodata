#!/usr/bin/env python3
"""
Generate a bash script that installs picodata build dependencies.

Build dependencies are taken from the packaging files of the source tree
and resolved by the native package manager:
  - rpm/picodata.spec: `dnf builddep` (on ALT Linux `rpmspec -q --buildrequires`,
                       its apt-get build-dep does not accept spec files)
  - debian/control:    `apt-get build-dep`

The generated script detects the current OS via /etc/os-release
(or takes the target name as the first argument) and installs the packages.
The source tree is taken from PICODATA_SRC_DIR, otherwise from the directory
of the generated script or the current directory, whichever contains it.
USE_DYNAMIC_BUILD from the environment is honored when evaluating the spec.

Usage:
  tools/gen_builddeps_script.py -o install_builddeps.sh
  tools/gen_builddeps_script.py --os fedora-43 --os ubuntu-noble
"""

import argparse
import pathlib
import shlex
import sys

# Not listed in the packaging files, but needed to build the binary:
# compiler toolchain, git (tarantool-sys takes its version from git),
# curl and CA certificates for `make install-cargo` (rustup over https).
BASE_PACKAGES = {
    "apt": ["build-essential", "git", "curl", "ca-certificates"],
    "apt-rpm": ["gcc", "gcc-c++", "make", "git", "curl", "ca-certificates"],
    # By file: on EL9/Fedora `curl` conflicts with preinstalled `curl-minimal`.
    "dnf": ["gcc", "gcc-c++", "make", "git", "/usr/bin/curl", "ca-certificates"],
}

# `detect` is a list of bash case patterns for "$ID|$VERSION_ID|$VERSION_CODENAME",
# words of $ID_LIKE are tried as $ID too (e.g. Rocky and Alma match CentOS).
# `extra_repo.match` are repo ids (as id components, e.g. `crb` matches `local-crb`)
# meaning the repo is already enabled, `extra_repo.enable` are globs to enable it.
OSES = {
    "altlinux-p9": {
        "pm": "apt-rpm",
        "detect": ["altlinux|p9|*", "altlinux|9|*", "altlinux|9.*|*"],
    },
    "altlinux-p10": {
        "pm": "apt-rpm",
        "detect": ["altlinux|p10|*", "altlinux|10|*", "altlinux|10.*|*"],
    },
    "altlinux-p11": {
        "pm": "apt-rpm",
        "detect": ["altlinux|p11|*", "altlinux|11|*", "altlinux|11.*|*"],
    },
    "astralinux-1.7": {
        "pm": "apt",
        "detect": ["astra|1.7*|*"],
    },
    "astralinux-1.8": {
        "pm": "apt",
        "detect": ["astra|1.8*|*"],
    },
    "rosa-chrome": {
        "pm": "dnf",
        "detect": ["rosa|*|*"],
    },
    "ubuntu-jammy": {
        "pm": "apt",
        "detect": ["ubuntu|*|jammy"],
    },
    "ubuntu-noble": {
        "pm": "apt",
        "detect": ["ubuntu|*|noble"],
    },
    "ubuntu-resolute": {
        "pm": "apt",
        "detect": ["ubuntu|*|resolute"],
    },
    "centos-8": {
        "pm": "dnf",
        "extra_repo": {
            "match": ["powertools", "codeready-builder"],
            "enable": ["powertools", "codeready-builder-for-rhel-8-*"],
        },
        "detect": ["centos|8|*", "centos|8.*|*", "rhel|8|*", "rhel|8.*|*"],
    },
    "centos-9": {
        "pm": "dnf",
        "extra_repo": {
            "match": ["crb", "codeready-builder"],
            "enable": ["crb", "codeready-builder-for-rhel-9-*"],
        },
        "detect": ["centos|9|*", "centos|9.*|*", "rhel|9|*", "rhel|9.*|*"],
    },
    "debian-bookworm": {
        "pm": "apt",
        "detect": ["debian|*|bookworm"],
    },
    "debian-trixie": {
        "pm": "apt",
        "detect": ["debian|*|trixie"],
    },
    "redos-7.3": {
        "pm": "dnf",
        "detect": ["redos|7.3*|*"],
    },
    "redos-8.0": {
        "pm": "dnf",
        "detect": ["redos|8*|*"],
    },
    "fedora-43": {
        "pm": "dnf",
        "detect": ["fedora|43|*"],
    },
    "fedora-44": {
        "pm": "dnf",
        "detect": ["fedora|44|*"],
    },
    "onyx-3": {
        "pm": "apt",
        "detect": ["osnova|3*|*", "onyx|3*|*", "*|*|onyx3"],
    },
}


# --------------------------------------------------------------------------
# bash script generation
# --------------------------------------------------------------------------


def unique(seq):
    return list(dict.fromkeys(seq))


def install_commands(info):
    pm = info["pm"]
    if pm not in BASE_PACKAGES:
        raise ValueError(f"unknown package manager: {pm}")
    pkgs = " ".join(shlex.quote(p) for p in BASE_PACKAGES[pm])
    if pm == "apt":
        return [
            "export DEBIAN_FRONTEND=noninteractive",
            "${SUDO} apt-get update",
            f"${{SUDO}} apt-get install -y --no-install-recommends {pkgs}",
            '${SUDO} apt-get build-dep -y --no-install-recommends "${SRC_DIR}"',
        ]
    if pm == "apt-rpm":
        return [
            "${SUDO} apt-get update",
            f"${{SUDO}} apt-get install -y {pkgs} rpmspec",
            'BUILD_REQUIRES="$(rpmspec -q --buildrequires "${SRC_DIR}/rpm/picodata.spec" | awk \'{print $1}\')"',
            "# shellcheck disable=SC2086",
            "${SUDO} apt-get install -y ${BUILD_REQUIRES}",
        ]
    # dnf
    cmds = ["PM=dnf"]
    repo = info.get("extra_repo")
    if repo:
        # Enable the repo only if no enabled repo (e.g. a local mirror) already provides it.
        cmds.append(f"if ! repo_enabled {' '.join(repo['match'])}; then")
        cmds.append('    ${SUDO} "${PM}" install -y dnf-plugins-core')
        cmds.append(f"    enable_repo {' '.join(shlex.quote(r) for r in repo['enable'])}")
        cmds.append("fi")
    cmds.append(f"${{SUDO}} \"${{PM}}\" install -y {pkgs} 'dnf-command(builddep)'")
    # sudo resets the environment, pass the variable used by the spec explicitly.
    cmds.append(
        '${SUDO} env "USE_DYNAMIC_BUILD=${USE_DYNAMIC_BUILD:-}" "${PM}" builddep -y "${SRC_DIR}/rpm/picodata.spec"'
    )
    return cmds


def render(oses):
    out = []
    w = out.append
    w("#!/bin/bash")
    w("# Generated by tools/gen_builddeps_script.py from rpm/picodata.spec and debian/control.")
    w("# Do not edit manually, regenerate instead.")
    w("#")
    w("# Usage: [USE_DYNAMIC_BUILD=1] $0 [--list | <os>]")
    w("#   without arguments the OS is detected via /etc/os-release")
    w("")
    w("# Functions are called in if-conditions on purpose, their failures are handled there.")
    w("# shellcheck disable=SC2310")
    w("")
    w("set -euo pipefail")
    w("")
    w(f'SUPPORTED="{" ".join(oses)}"')
    w("")
    w('SUDO=""')
    w('UID_NUM="$(id -u)"')
    w('[[ "${UID_NUM}" -ne 0 ]] && SUDO="sudo"')
    w("")
    w("# Usage: repo_enabled <id>... - whether an enabled repo id contains one of <id> as a component.")
    w("repo_enabled() {")
    w("    local ids id")
    w('    ids="$("${PM}" repolist --enabled 2>/dev/null | awk \'NR > 1 {print $1}\')" || return 1')
    w('    for id in "$@"; do')
    w('        grep -qiE "(^|[-_.])${id}([-_.]|$)" <<< "${ids}" && return 0')
    w("    done")
    w("    return 1")
    w("}")
    w("")
    w("# Usage: enable_repo <glob>... - enable the first existing repo.")
    w("enable_repo() {")
    w("    local repo")
    w('    for repo in "$@"; do')
    w('        ${SUDO} "${PM}" config-manager --set-enabled "${repo}" 2>/dev/null && return 0')
    w("    done")
    w('    echo "Cannot enable any of the repos: $*" >&2')
    w("    return 1")
    w("}")
    w("")
    w("detect_os() {")
    w("    [[ -r /etc/os-release ]] || return 1")
    w("    local id ids")
    w("    # shellcheck disable=SC1091")
    w("    . /etc/os-release")
    w('    read -ra ids <<< "${ID:-} ${ID_LIKE:-}"')
    w('    for id in "${ids[@]}"; do')
    w('        case "${id}|${VERSION_ID:-}|${VERSION_CODENAME:-}" in')
    for name in oses:
        w(f"            {'|'.join(_quote_pattern(p) for p in OSES[name]['detect'])}) echo {name}; return 0 ;;")
    w("            *) ;;")
    w("        esac")
    w("    done")
    w("    return 1")
    w("}")
    w("")
    w("# Prints the absolute path of the first directory containing the picodata sources.")
    w("find_src_dir() {")
    w("    local dir candidates")
    w('    if [[ -n "${PICODATA_SRC_DIR:-}" ]]; then')
    w('        candidates=("${PICODATA_SRC_DIR}")')
    w("    else")
    w("        # BASH_SOURCE is unset when the script is read from stdin.")
    w('        candidates=("$(dirname -- "${BASH_SOURCE[0]:-.}")" "${PWD}")')
    w("    fi")
    w('    for dir in "${candidates[@]}"; do')
    w('        if [[ -f "${dir}/rpm/picodata.spec" && -f "${dir}/debian/control" ]]; then')
    w('            (CDPATH="" cd -- "${dir}" && pwd -P)')
    w("            return")
    w("        fi")
    w("    done")
    w('    echo "picodata sources not found in: ${candidates[*]}, set PICODATA_SRC_DIR" >&2')
    w("    return 1")
    w("}")
    w("")
    w('case "${1:-}" in')
    w("    -h|--help)")
    w('        echo "Usage: [USE_DYNAMIC_BUILD=1] $0 [--list | <os>]"')
    w('        echo "Supported: ${SUPPORTED}"')
    w("        exit 0 ;;")
    w("    --list)")
    w('        tr " " "\\n" <<< "${SUPPORTED}"')
    w("        exit 0 ;;")
    w("    '')")
    w('        if ! TARGET="$(detect_os)"; then')
    w('            echo "Cannot detect OS, pass it as an argument. Supported: ${SUPPORTED}" >&2')
    w("            exit 1")
    w("        fi ;;")
    w("    *)")
    w('        TARGET="$1" ;;')
    w("esac")
    w("")
    w('if ! SRC_DIR="$(find_src_dir)"; then')
    w("    exit 1")
    w("fi")
    w("")
    w('echo "Installing build dependencies for ${TARGET} from ${SRC_DIR}"')
    w("")
    w('case "${TARGET}" in')
    for name in oses:
        w(f"    {name})")
        for cmd in install_commands(OSES[name]):
            w(f"        {cmd}")
        w("        ;;")
    w("    *)")
    w('        echo "Unsupported OS: ${TARGET}. Supported: ${SUPPORTED}" >&2')
    w("        exit 1 ;;")
    w("esac")
    w("")
    return "\n".join(out)


def _quote_pattern(pattern):
    # Keep `*` as a glob, escape the rest (`|` and `.` are literal in the subject).
    return "".join(c if c == "*" else ("\\|" if c == "|" else c) for c in pattern)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-o", "--output", type=pathlib.Path, help="output file (default: stdout)")
    parser.add_argument(
        "--os",
        action="append",
        choices=list(OSES),
        metavar="OS",
        help="generate only for this OS (repeatable). Default: all",
    )
    args = parser.parse_args()

    oses = unique(args.os) if args.os else list(OSES)
    script = render(oses)
    if args.output:
        args.output.write_text(script)
        args.output.chmod(0o755)
    else:
        sys.stdout.write(script)


if __name__ == "__main__":
    main()
