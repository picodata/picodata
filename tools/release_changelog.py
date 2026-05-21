#!/usr/bin/env python3
"""Generate CHANGELOG.md and RELEASE_NOTES.md sections for an upcoming tag.

Run this on a release branch (e.g., `26.2`) when preparing X.Y.Z:

    tools/release_changelog.py 26.2.2

Steps:
    1. Validate the working tree is clean and the tag does not yet exist.
    2. Generate a CHANGELOG block via `git-pico-cliff --tag X.Y.Z`.
    3. Generate a RELEASE_NOTES block via the `release_notes` Python renderer.
    4. Splice both blocks into CHANGELOG.md and RELEASE_NOTES.md.
    5. `git rm` the consumed fragments from `release_notes/unreleased/`.
    6. Stage the modified files.

CHANGELOG.md / RELEASE_NOTES.md are *not* updated by CI — this script is the
entry point.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools import release_notes
elif __package__:
    from . import release_notes
else:
    import release_notes

TAG_RE = re.compile(r"^\d+\.\d+\.\d+$")
BRANCH_RE = re.compile(r"^\d+\.\d+$")


def _run(cmd: list[str], *, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def _git(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["git", *args], check=check, capture=capture)


def _die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(code)


def _warn(msg: str) -> None:
    print(f"warning: {msg}", file=sys.stderr)


def _check_clean_tree() -> None:
    out = _git("status", "--porcelain").stdout
    if out.strip():
        _die("working tree is not clean; commit or stash changes first.")


def _check_tag_absent(tag: str) -> None:
    r = _git("rev-parse", "--verify", "--quiet", f"refs/tags/{tag}", check=False)
    if r.returncode == 0:
        _die(f"tag {tag} already exists.")


def _current_branch() -> str:
    return _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()


# File structure expected by the splice:
#   H1 (`# Title`)         — file title (`# Changelog` / `# Release Notes`)
#   H2 (`## [VERSION] - DATE`) — each release section (both files)
#   H2 (`## [unreleased]`) or
#   H2 (`## [X.Y.Z] - Unreleased`) — legacy hand-written unreleased block.
#     Recognised so the first automated run replaces it in place rather
#     than duplicating; once consumed, only the H2-prepend path is taken.
_UNRELEASED_PATTERNS = (
    re.compile(r"^## \[unreleased\][^\n]*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^## \[\d[\d.]*\][^\n]* - [Uu]nreleased[^\n]*$", re.MULTILINE),
)
_NEXT_H2_RE = re.compile(r"^## \[", re.MULTILINE)
_H1_RE = re.compile(r"^# .+$", re.MULTILINE)
_TAG_HEADER_RE = re.compile(
    r"^## \[(?P<tag>\d[\d.]*)\][^\n]* - \d{4}-\d{2}-\d{2}",
    re.MULTILINE,
)


def _splice(target_text: str, block: str) -> str:
    """Insert `block` into `target_text` as a release section.

    Idempotent: if `block` already names a `[TAG]` section that is present
    in `target_text`, return `target_text` unchanged.

    Replaces a legacy `[unreleased]` or `[X.Y.Z] - Unreleased` block if
    found; otherwise inserts `block` before the first existing release
    section (preserving any preamble between the H1 and that section).
    """
    block = block.strip()

    fresh_tag = _TAG_HEADER_RE.search(block)
    if fresh_tag:
        present = {m.group("tag") for m in _TAG_HEADER_RE.finditer(target_text)}
        if fresh_tag.group("tag") in present:
            return target_text

    for pat in _UNRELEASED_PATTERNS:
        m = pat.search(target_text)
        if m:
            next_h2 = _NEXT_H2_RE.search(target_text, m.end())
            end = next_h2.start() if next_h2 else len(target_text)
            before = target_text[: m.start()].rstrip()
            after = target_text[end:].lstrip("\n")
            return (before + "\n\n" + block + "\n\n" + after).rstrip() + "\n"

    h1 = _H1_RE.search(target_text)
    if h1:
        # Preserve the preamble (e.g. "All notable changes..." + badge) that
        # lives between the H1 and the first release section by inserting
        # immediately before that first section rather than right after H1.
        first_release = _TAG_HEADER_RE.search(target_text, h1.end())
        if first_release:
            insertion_point = first_release.start()
            head = target_text[:insertion_point].rstrip("\n")
            tail = target_text[insertion_point:].lstrip("\n")
            return (head + "\n\n" + block + "\n\n" + tail).rstrip() + "\n"
        # No existing release section. Append at end so the preamble that
        # lives between the H1 and end-of-file stays where it is.
        # On the next run, `first_release` will be set and the block above
        # will insert before it.
        head = target_text.rstrip("\n")
        return (head + "\n\n" + block).rstrip() + "\n"

    return (block + "\n\n" + target_text).rstrip() + "\n"


def main() -> int:
    p = argparse.ArgumentParser(
        description="Generate CHANGELOG and RELEASE_NOTES sections for TAG.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("tag", help="release tag, e.g. 26.2.2")
    args = p.parse_args()

    if not TAG_RE.match(args.tag):
        _die(f"TAG must look like CalVer X.Y.Z (e.g., 26.2.2); got {args.tag!r}")

    root = Path(_git("rev-parse", "--show-toplevel").stdout.strip())

    branch = _current_branch()
    if not BRANCH_RE.match(branch):
        _warn(f"current branch {branch!r} is not a release branch (expected `X.Y`); continuing anyway.")

    _check_clean_tree()
    _check_tag_absent(args.tag)

    release_notes_md = root / "RELEASE_NOTES.md"
    if not release_notes_md.exists():
        release_notes_md.write_text(
            "# Release Notes\n\n"
            "User-facing changelog of picodata releases, compiled from\n"
            "per-MR fragments in `release_notes/unreleased/` at release\n"
            "time. See `doc/dev/generating-changelog.md` for the release\n"
            "flow.\n\n",
            encoding="utf-8",
        )

    fresh_changelog = root / "CHANGELOG.md.fresh"
    try:
        print(f"==> generating CHANGELOG block for {args.tag}", flush=True)
        _run(["git-pico-cliff", "--tag", args.tag, "--unreleased", "-o", str(fresh_changelog)], capture=False)

        print(f"==> generating RELEASE_NOTES block for {args.tag}", flush=True)
        try:
            fresh_notes = release_notes.render_release_notes_section(tag=args.tag, repo_root=root)
        except release_notes.ReleaseNotesError as e:
            _die(str(e))

        print("==> splicing CHANGELOG.md and RELEASE_NOTES.md", flush=True)
        fresh_blocks = [
            ("CHANGELOG.md", fresh_changelog.read_text(encoding="utf-8")),
            ("RELEASE_NOTES.md", fresh_notes),
        ]
        for target_name, fresh_block in fresh_blocks:
            target_path = root / target_name
            target_path.write_text(
                _splice(
                    target_path.read_text(encoding="utf-8"),
                    fresh_block,
                ),
                encoding="utf-8",
            )
    finally:
        fresh_changelog.unlink(missing_ok=True)

    # Consume the fragments that contributed to the RELEASE_NOTES section.
    fragment_dir = root / "release_notes" / "unreleased"
    consumed = sorted(p for p in fragment_dir.glob("*.md") if p.name.lower() != "readme.md")
    if consumed:
        print(f"==> removing {len(consumed)} consumed fragment(s) from release_notes/unreleased/", flush=True)
        _git("rm", "--quiet", *[str(p.relative_to(root)) for p in consumed], capture=False)

    _git("add", "CHANGELOG.md", "RELEASE_NOTES.md")

    if _git("diff", "--cached", "--quiet", check=False).returncode == 0:
        print("==> nothing to stage; no changes since the previous tag.", flush=True)
        return 0

    print(f"\n==> done. CHANGELOG.md and RELEASE_NOTES.md staged for {args.tag}. Edit freely, then commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
