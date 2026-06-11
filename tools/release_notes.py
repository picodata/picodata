#!/usr/bin/env python3
"""Render a RELEASE_NOTES section from per-MR changelog fragments.

Picodata follows the fragment-per-change model adapted from Tarantool and
GitLab. Each MR with user-visible impact lands a small Markdown file under

    release_notes/unreleased/<slug>.md

with a single H2 header naming the type and (optionally) a category,
followed by one or more bullets and an optional `----` separator with a
note for the release manager:

    ## feat/sql

    - Added equality-facts analysis pass over the relational plan ([!2901]).
    - Optimize an extra subplan clone in single-replicaset dispatch.

    ----

    Notable: planner now exploits equality facts across motion boundaries.

Modes:

    release_notes.py --unreleased            # header: `## [unreleased]`
    release_notes.py --tag X.Y.Z             # header: `## [X.Y.Z] - DATE`
    release_notes.py --check [PATHS...]      # validate fragments, no render

`--check` parses each fragment without rendering, git lookups, or MR-ref
warnings; it exits non-zero if any fragment is structurally malformed
(wrong header level, unknown type, empty body). With no PATHS it checks
every fragment in `--fragment-dir`; the pre-commit hook passes the staged
fragment paths directly.

Output goes to stdout by default; pass -o PATH to write to a file. The
same renderer is importable from Python; consumed fragments are removed
by `tools/release_changelog.py`, not by this script.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# 10 sections in the same order as CHANGELOG.md (Breaking changes hoisted).
TYPE_ORDER: tuple[tuple[str, str], ...] = (
    ("breaking", "Breaking changes"),
    ("feat", "Features"),
    ("fix", "Bug fixes"),
    ("refactor", "Refactor"),
    ("docs", "Documentation"),
    ("perf", "Performance"),
    ("test", "Testing"),
    ("chore", "Miscellaneous Tasks"),
    ("revert", "Revert"),
    ("build", "Build"),
)
ALLOWED_TYPES: frozenset[str] = frozenset(t for t, _ in TYPE_ORDER)

DEFAULT_FRAGMENT_DIR = Path("release_notes/unreleased")

REPO_URL = "https://git.picodata.io/core/picodata"
PROJECT_REPO_URLS = {
    "tarantool": "https://git.picodata.io/core/tarantool",
}

HEADER_RE = re.compile(r"^##\s+([A-Za-z]+)\s*(?:/\s*(.+?))?\s*$", re.MULTILINE)
SEPARATOR_RE = re.compile(r"^----+\s*$", re.MULTILINE)
_MR_SHORTHAND_RE = re.compile(r"\[!(\d+)\](?!\()")
_PROJECT_MR_SHORTHAND_RE = re.compile(r"\[([A-Za-z][A-Za-z0-9_-]*)!(\d+)\](?!\()")
_COMMIT_MR_RES = (
    re.compile(r"^Part-of:\s+\S*!(\d+)", re.MULTILINE),
    re.compile(r"[Ss]ee merge request\s+\S*!(\d+)"),
)


class ReleaseNotesError(Exception):
    """Raised when release-note fragments cannot be rendered."""


@dataclass
class Fragment:
    path: Path
    type: str
    category: str | None
    body: str
    note: str | None
    mr_number: int | None = None


def _warn(msg: str) -> None:
    print(f"warning: {msg}", file=sys.stderr)


def _git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.run(
        ["git", *args],
        check=True,
        text=True,
        capture_output=True,
        cwd=cwd,
    ).stdout


def _git_or_none(*args: str, cwd: Path | None = None) -> str | None:
    r = subprocess.run(["git", *args], text=True, capture_output=True, cwd=cwd)
    return r.stdout.strip() if r.returncode == 0 else None


def _expand_mr_refs(text: str) -> str:
    """Expand GitLab MR shorthand to full Markdown links."""
    text = _MR_SHORTHAND_RE.sub(
        lambda m: f"[!{m.group(1)}]({REPO_URL}/-/merge_requests/{m.group(1)})",
        text,
    )

    def expand_project_mr(m: re.Match[str]) -> str:
        project = m.group(1)
        mr = m.group(2)
        repo_url = PROJECT_REPO_URLS.get(project)
        if repo_url is None:
            return m.group(0)
        return f"[{project}!{mr}]({repo_url}/-/merge_requests/{mr})"

    return _PROJECT_MR_SHORTHAND_RE.sub(expand_project_mr, text)


def _find_commit_mr(path: Path, *, repo_root: Path | None = None) -> int | None:
    """Return the MR IID of the commit that first added *path*, or None."""
    git_path = path
    if repo_root is not None:
        try:
            git_path = path.relative_to(repo_root)
        except ValueError:
            git_path = path
    body = _git_or_none(
        "log",
        "--diff-filter=A",
        "--follow",
        "-1",
        "--format=%B",
        "--",
        str(git_path),
        cwd=repo_root,
    )
    if not body:
        return None
    for pat in _COMMIT_MR_RES:
        m = pat.search(body)
        if m:
            return int(m.group(1))
    return None


def tag_date(tag: str, *, repo_root: Path | None = None) -> str:
    out = _git_or_none("log", "-1", "--format=%aI", tag, cwd=repo_root) or ""
    return out[:10] or date.today().isoformat()


def parse_fragment(path: Path) -> Fragment:
    text = path.read_text(encoding="utf-8")
    m = HEADER_RE.search(text)
    if not m:
        raise ValueError(f"{path}: no `## <type>[/<category>]` H2 heading found")
    type_ = m.group(1).strip().lower()
    # Accept `doc` as an alias for `docs`. The cliff regex `^docs?` also
    # accepts both, and the pre-commit hook allows both — fragments should
    # not be more restrictive than the commit grammar.
    if type_ == "doc":
        type_ = "docs"
    if type_ not in ALLOWED_TYPES:
        types = ", ".join(t for t, _ in TYPE_ORDER)
        raise ValueError(f"{path}: unknown type `{type_}`. Allowed: {types}")
    category = (m.group(2) or "").strip() or None

    rest = text[m.end() :]
    sep = SEPARATOR_RE.search(rest)
    if sep:
        body = rest[: sep.start()].strip()
        note = rest[sep.end() :].strip() or None
    else:
        body = rest.strip()
        note = None
    if not body:
        raise ValueError(f"{path}: fragment body is empty")
    return Fragment(path=path, type=type_, category=category, body=body, note=note)


def collect_fragments(fragment_dir: Path, *, repo_root: Path | None = None) -> list[Fragment]:
    if not fragment_dir.is_dir():
        return []
    fragments: list[Fragment] = []
    errors: list[str] = []
    for p in sorted(fragment_dir.glob("*.md")):
        if p.name.lower() == "readme.md":
            continue
        try:
            f = parse_fragment(p)
            f.mr_number = _find_commit_mr(p, repo_root=repo_root)
            fragments.append(f)
        except ValueError as e:
            errors.append(str(e))
    if errors:
        raise ReleaseNotesError("\n".join(errors))
    return fragments


def validate_fragments(paths: Sequence[Path]) -> list[str]:
    """Return one error message per structurally malformed fragment.

    Each path is parsed with `parse_fragment`. Unlike `collect_fragments`,
    this performs no git/MR lookups and emits no warnings, so it is safe to
    run on an arbitrary file list (the pre-commit hook passes staged
    fragments directly). A well-formed fragment yields no message.
    """
    errors: list[str] = []
    for path in paths:
        if path.name.lower() == "readme.md":
            continue
        try:
            parse_fragment(path)
        except ValueError as e:
            errors.append(str(e))
        except OSError as e:
            errors.append(f"{path}: {e}")
    return errors


def render(header: str, fragments: list[Fragment], *, warn: Callable[[str], None] = _warn) -> str:
    by_type: dict[str, list[Fragment]] = defaultdict(list)
    notes: list[tuple[str, str]] = []
    for f in fragments:
        by_type[f.type].append(f)
        if f.note:
            notes.append((f.path.name, f.note))

    out: list[str] = [header, ""]

    # Release-manager notes live inside the section, immediately after the
    # header. This keeps each section's notes co-located with its content,
    # so accumulated un-merged release sections don't interleave their
    # comments at the top of the file.
    if notes:
        out.append("<!--")
        out.append("Release-manager notes (remove before publishing):")
        for name, note in notes:
            out.append("")
            out.append(f"- {name}:")
            for line in note.splitlines():
                out.append(f"    {line}")
        out.append("-->")
        out.append("")

    emitted = False
    for type_key, type_label in TYPE_ORDER:
        items = by_type.get(type_key)
        if not items:
            continue
        emitted = True
        out.append(f"### {type_label}")
        out.append("")

        # No-category fragments first, then by category name.
        by_cat: dict[str | None, list[Fragment]] = defaultdict(list)
        for f in items:
            by_cat[f.category].append(f)
        cats_sorted = sorted(
            by_cat.keys(),
            key=lambda c: (1, c) if c else (0, ""),
        )
        for cat in cats_sorted:
            cat_items = sorted(by_cat[cat], key=lambda f: f.path.name)
            if cat is not None:
                out.append(f"#### {cat}")
                out.append("")
            for f in cat_items:
                body = _expand_mr_refs(f.body.strip())
                # Auto-detected MR numbers (from the commit that added the
                # fragment) are NOT appended to the body: doing so makes
                # multi-bullet fragments look like only the last bullet
                # came from the MR. Warn instead so the author can add an
                # inline `[!N]` reference in a follow-up.
                if f.mr_number is not None and f"[!{f.mr_number}]" not in body:
                    warn(
                        f"{f.path.name}: no inline MR reference; "
                        f"add `[!{f.mr_number}]` to the fragment "
                        f"(detected from the introducing commit)."
                    )
                out.append(body)
                out.append("")
    if not emitted:
        out.append("_No user-facing changes in this range._")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def render_release_notes_section(
    *,
    tag: str | None = None,
    unreleased: bool = False,
    fragment_dir: Path | str = DEFAULT_FRAGMENT_DIR,
    repo_root: Path | None = None,
    warn: Callable[[str], None] = _warn,
) -> str:
    """Render a RELEASE_NOTES section for Python callers."""
    if (tag is None) != unreleased:
        raise ReleaseNotesError("exactly one of `tag` or `unreleased` must be set")

    if repo_root is None:
        repo_root = Path(_git("rev-parse", "--show-toplevel").strip())

    fragment_path = Path(fragment_dir)
    if not fragment_path.is_absolute():
        fragment_path = repo_root / fragment_path

    if unreleased:
        header = "## [unreleased]"
    else:
        assert tag is not None
        normalized_tag = tag.lstrip("v")
        header = f"## [{normalized_tag}] - {tag_date(tag, repo_root=repo_root)}"

    fragments = collect_fragments(fragment_path, repo_root=repo_root)
    return render(header, fragments, warn=warn)


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Render a RELEASE_NOTES section from changelog fragments.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--unreleased", action="store_true", help="header: `## [unreleased]`")
    mode.add_argument(
        "--tag", metavar="TAG", help="header: `## [TAG] - DATE` (DATE = tag date if it exists, else today)"
    )
    mode.add_argument(
        "--check", action="store_true", help="validate fragments structurally and exit non-zero on any error"
    )
    p.add_argument(
        "--fragment-dir",
        type=Path,
        default=DEFAULT_FRAGMENT_DIR,
        help=f"directory with *.md fragments (default: {DEFAULT_FRAGMENT_DIR})",
    )
    p.add_argument("-o", "--output", default="-", help="output file (default: stdout)")
    p.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="fragment files to validate (only with --check); default: all *.md in --fragment-dir",
    )
    args = p.parse_args(argv)

    if args.check:
        if args.paths:
            targets = list(args.paths)
        elif args.fragment_dir.is_dir():
            targets = sorted(args.fragment_dir.glob("*.md"))
        else:
            targets = []
        errors = validate_fragments(targets)
        if errors:
            for msg in errors:
                for line in msg.splitlines():
                    print(f"error: {line}", file=sys.stderr)
            return 1
        print(f"ok: {len(targets)} release-note fragment(s) are well-formed")
        return 0

    try:
        output = render_release_notes_section(
            tag=args.tag,
            unreleased=args.unreleased,
            fragment_dir=args.fragment_dir,
        )
    except ReleaseNotesError as e:
        for line in str(e).splitlines():
            print(f"error: {line}", file=sys.stderr)
        return 1

    if args.output == "-":
        sys.stdout.write(output)
    else:
        Path(args.output).write_text(output, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
