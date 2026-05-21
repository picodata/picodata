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
