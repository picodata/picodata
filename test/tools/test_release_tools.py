from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import release_changelog, release_notes  # noqa: E402


def _write_fragment(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).lstrip(), encoding="utf-8")


def test_release_notes_renderer_is_importable(tmp_path: Path) -> None:
    fragment_dir = tmp_path / "release_notes" / "unreleased"
    _write_fragment(
        fragment_dir / "sql-feature.md",
        """
        ## feat/sql

        - Added equality facts ([!123]).
        """,
    )
    _write_fragment(
        fragment_dir / "tarantool-fix.md",
        """
        ## fix

        - Fixed tuple decoding ([tarantool!55]).
        """,
    )

    warnings: list[str] = []
    output = release_notes.render_release_notes_section(
        unreleased=True,
        fragment_dir=fragment_dir,
        repo_root=tmp_path,
        warn=warnings.append,
    )

    assert warnings == []
    assert (
        output
        == dedent(
            """
        ## [unreleased]

        ### Features

        #### sql

        - Added equality facts ([!123](https://git.picodata.io/core/picodata/-/merge_requests/123)).

        ### Bug fixes

        - Fixed tuple decoding ([tarantool!55](https://git.picodata.io/core/tarantool/-/merge_requests/55)).
        """
        ).lstrip()
    )


def test_release_notes_cli_matches_importable_renderer(tmp_path: Path) -> None:
    fragment_dir = tmp_path / "fragments"
    _write_fragment(
        fragment_dir / "fix.md",
        """
        ## fix/cli

        - Fixed CLI output ([!321]).
        """,
    )

    expected = release_notes.render_release_notes_section(
        unreleased=True,
        fragment_dir=fragment_dir,
        repo_root=ROOT,
    )
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "release_notes.py"),
            "--unreleased",
            "--fragment-dir",
            str(fragment_dir),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    assert result.stderr == ""
    assert result.stdout == expected


def test_release_notes_parse_errors_are_exceptions_and_cli_errors(tmp_path: Path) -> None:
    fragment_dir = tmp_path / "fragments"
    _write_fragment(
        fragment_dir / "bad.md",
        """
        # Missing required H2

        - Body.
        """,
    )

    with pytest.raises(release_notes.ReleaseNotesError, match="no `## <type>\\[/<category>\\]` H2"):
        release_notes.render_release_notes_section(
            unreleased=True,
            fragment_dir=fragment_dir,
            repo_root=tmp_path,
        )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "release_notes.py"),
            "--unreleased",
            "--fragment-dir",
            str(fragment_dir),
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "error:" in result.stderr
    assert "no `## <type>[/<category>]` H2" in result.stderr


def test_validate_fragments_flags_only_structural_errors(tmp_path: Path) -> None:
    good = tmp_path / "good.md"
    _write_fragment(
        good,
        """
        ## feat/sql

        - A change without any inline MR reference.
        """,
    )
    single_hash = tmp_path / "single-hash.md"
    _write_fragment(
        single_hash,
        """
        # feat/sql

        - Header uses one hash instead of two.
        """,
    )
    unknown_type = tmp_path / "unknown-type.md"
    _write_fragment(
        unknown_type,
        """
        ## wibble

        - Unknown change type.
        """,
    )
    empty_body = tmp_path / "empty-body.md"
    _write_fragment(empty_body, "## fix\n")

    # A well-formed fragment passes even without an inline MR reference:
    # a missing `[!N]` is a render-time warning, not a structural error.
    assert release_notes.validate_fragments([good]) == []

    errors = release_notes.validate_fragments([single_hash, unknown_type, empty_body])
    assert len(errors) == 3
    assert any("no `## <type>[/<category>]` H2" in e for e in errors)
    assert any("unknown type `wibble`" in e for e in errors)
    assert any("fragment body is empty" in e for e in errors)


def test_release_notes_check_cli(tmp_path: Path) -> None:
    fragment_dir = tmp_path / "fragments"
    _write_fragment(
        fragment_dir / "ok.md",
        """
        ## feat

        - A well-formed fragment ([!1]).
        """,
    )

    def run_check(*extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(ROOT / "tools" / "release_notes.py"), "--check", *extra],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )

    ok = run_check("--fragment-dir", str(fragment_dir))
    assert ok.returncode == 0
    assert ok.stderr == ""
    assert "well-formed" in ok.stdout

    _write_fragment(
        fragment_dir / "bad.md",
        """
        # feat

        - Single-hash header.
        """,
    )

    # Whole-directory scan (the CI invocation) reports the bad fragment.
    bad_dir = run_check("--fragment-dir", str(fragment_dir))
    assert bad_dir.returncode == 1
    assert bad_dir.stdout == ""
    assert "error:" in bad_dir.stderr
    assert "no `## <type>[/<category>]` H2" in bad_dir.stderr

    # Explicit paths (how the pre-commit hook invokes it) are validated too.
    bad_path = run_check(str(fragment_dir / "bad.md"))
    assert bad_path.returncode == 1
    assert "error:" in bad_path.stderr


def test_release_changelog_calls_release_notes_renderer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n", encoding="utf-8")
    (tmp_path / "RELEASE_NOTES.md").write_text("# Release Notes\n\n", encoding="utf-8")
    (tmp_path / "release_notes" / "unreleased").mkdir(parents=True)

    run_calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
        run_calls.append(cmd)
        assert cmd[0] == "git-pico-cliff"
        output = Path(cmd[cmd.index("-o") + 1])
        output.write_text(
            "## [26.2.2] - 2026-05-22\n\n### Features\n\n- Generated changelog item.\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(cmd, 0, "", "")

    def fake_git(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = ["git", *args]
        if args == ("rev-parse", "--show-toplevel"):
            return subprocess.CompletedProcess(cmd, 0, f"{tmp_path}\n", "")
        if args == ("rev-parse", "--abbrev-ref", "HEAD"):
            return subprocess.CompletedProcess(cmd, 0, "26.2\n", "")
        if args == ("status", "--porcelain"):
            return subprocess.CompletedProcess(cmd, 0, "", "")
        if args == ("rev-parse", "--verify", "--quiet", "refs/tags/26.2.2"):
            return subprocess.CompletedProcess(cmd, 1, "", "")
        if args == ("add", "CHANGELOG.md", "RELEASE_NOTES.md"):
            return subprocess.CompletedProcess(cmd, 0, "", "")
        if args == ("diff", "--cached", "--quiet"):
            return subprocess.CompletedProcess(cmd, 1, "", "")
        raise AssertionError(f"unexpected git call: {args}")

    render_calls: list[tuple[str | None, Path | None]] = []

    def fake_render_release_notes_section(
        *,
        tag: str | None = None,
        unreleased: bool = False,
        fragment_dir: Path | str = release_notes.DEFAULT_FRAGMENT_DIR,
        repo_root: Path | None = None,
        warn=release_notes._warn,
    ) -> str:
        assert unreleased is False
        assert fragment_dir == release_notes.DEFAULT_FRAGMENT_DIR
        render_calls.append((tag, repo_root))
        return "## [26.2.2] - 2026-05-22\n\n### Bug fixes\n\n- Generated release-note item.\n"

    monkeypatch.setattr(release_changelog, "_run", fake_run)
    monkeypatch.setattr(release_changelog, "_git", fake_git)
    monkeypatch.setattr(
        release_changelog.release_notes,
        "render_release_notes_section",
        fake_render_release_notes_section,
    )
    monkeypatch.setattr(sys, "argv", ["release_changelog.py", "26.2.2"])

    assert release_changelog.main() == 0

    assert render_calls == [("26.2.2", tmp_path)]
    assert run_calls == [
        ["git-pico-cliff", "--tag", "26.2.2", "--unreleased", "-o", str(tmp_path / "CHANGELOG.md.fresh")]
    ]
    assert "Generated changelog item." in (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "Generated release-note item." in (tmp_path / "RELEASE_NOTES.md").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "tag",
    [
        "26.2.2",
        "0.0.0",
        "99.99.99",
        "26.2.2-rc1",
        "26.2.2-alpha",
        "26.2.2-beta.1",
        "26.2.2-rc.1.2",
        "26.2.2-0abc",
    ],
)
def test_tag_re_accepts_valid_tags(tag: str) -> None:
    assert release_changelog.TAG_RE.match(tag), f"TAG_RE should match {tag!r}"


@pytest.mark.parametrize(
    "tag",
    [
        "26.2.2-",  # trailing dash, no identifier
        "26.2.2-rc1.",  # trailing dot
        "26.2.2-rc1-",  # trailing dash after identifier
        "26.2.2-rc1..2",  # double dot
        "26.2.2.",  # trailing dot on version
        "v26.2.2",  # v prefix
        "26.2",  # missing patch
        "26.2.2-rc.1.",  # trailing dot after dot-separated identifiers
        "26.2.2-rc.1-",  # trailing dash after dot-separated identifiers
    ],
)
def test_tag_re_rejects_invalid_tags(tag: str) -> None:
    assert not release_changelog.TAG_RE.match(tag), f"TAG_RE should NOT match {tag!r}"


def _make_changelog(*sections: str) -> str:
    """Build a minimal CHANGELOG.md body from section blocks."""
    return "# Changelog\n\n" + "\n".join(sections).rstrip() + "\n"


def _section(tag: str, date: str = "2026-01-01") -> str:
    return f"## [{tag}] - {date}\n\n### Features\n\n- Item from {tag}.\n"


def test_splice_recognises_rc_tag_as_existing_section() -> None:
    """Duplicate check works with pre-release tags."""
    target = _make_changelog(
        _section("26.2.1"),
        _section("26.2.2-rc1"),
    )
    block = _section("26.2.2-rc1", "2026-05-22")
    assert release_changelog._splice(target, block) == target


def test_splice_recognises_stable_tag_when_rc_sections_present() -> None:
    """Duplicate check for stable tag works even when only rc sections exist."""
    target = _make_changelog(_section("26.2.2-rc1"))
    block = _section("26.2.2-rc1", "2026-05-22")
    assert release_changelog._splice(target, block) == target


def test_splice_prepends_rc_before_older_stable() -> None:
    """A pre-release for a newer version goes above the older stable."""
    target = _make_changelog(_section("26.2.1"))
    block = _section("26.2.2-rc1")
    result = release_changelog._splice(target, block)
    assert result.index("[26.2.2-rc1]") < result.index("[26.2.1]")


def test_splice_prepends_stable_before_its_rc() -> None:
    """When stable is added after its rc, it lands on top (newest first)."""
    target = _make_changelog(
        _section("26.2.2-rc1"),
        _section("26.2.1"),
    )
    block = _section("26.2.2")
    result = release_changelog._splice(target, block)
    idx_stable = result.index("[26.2.2]")
    idx_rc = result.index("[26.2.2-rc1]")
    assert idx_stable < idx_rc


def test_splice_rc_ordering_with_multiple_rcs() -> None:
    """Multiple rc releases interleave correctly with stables from other minors."""
    target = _make_changelog(
        _section("26.2.2"),
        _section("26.2.2-rc2"),
        _section("26.2.2-rc1"),
        _section("26.2.1"),
    )
    # Add a new rc for the next patch
    block = _section("26.2.3-rc1")
    result = release_changelog._splice(target, block)
    lines = result.splitlines()
    tag_lines = [line for line in lines if line.startswith("## [")]
    assert tag_lines[0].startswith("## [26.2.3-rc1]")
    # Existing order preserved below the new top entry
    assert tag_lines[1].startswith("## [26.2.2]")
    assert tag_lines[2].startswith("## [26.2.2-rc2]")
    assert tag_lines[3].startswith("## [26.2.2-rc1]")
