"""
Unit tests for the version registry (`framework.registry.Registry`) and the
`framework.util.build.Executable` entity. Exercises filtering of Git tags,
alias-assignment, resolution, and `.0`-semantics rules used by the rolling
upgrade suite.
"""

from framework.registry import Registry
from framework.util.version import VersionAlias

from pathlib import Path
from packaging.version import Version
import pytest


LOCAL_BUILD = Path("/fake/target/debug/picodata")


def make_registry(
    current_version: str,
    available_tags: list[str],
    executable_list: frozenset[str] = frozenset(),
    err_on_missing_binaries: bool = False,
    local_build_path: Path = LOCAL_BUILD,
) -> Registry:
    """
    Construct a `Registry` from static facts. `executable_list` is the set of
    `picodata-<version>` names that the fake `binary_lookup` will report as
    present; everything else returns `None`.
    """

    def binary_lookup(version: Version) -> Path | None:
        name = f"picodata-{version}"
        return Path(f"/fake/bin/{name}") if name in executable_list else None

    return Registry(
        current_version=Version(current_version),
        tags=[Version(tag) for tag in available_tags],
        local_build_path=local_build_path,
        binary_lookup=binary_lookup,
        err_on_missing_binaries=err_on_missing_binaries,
    )


def test_fetch_filters_inappropriate_versions():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.0", "25.5.1", "25.5.2rc1", "25.4.1"],
    )
    versions = [str(e.version) for e in registry.executables]
    assert "25.5.3" in versions  # Passed: prepended current.
    assert "25.5.1" in versions  # Passed: valid and previous.
    assert "25.4.1" in versions  # Passed: valid and previous.
    assert "25.5.0" not in versions  # Filtered: zero by patch.
    assert "25.5.2rc1" not in versions  # Filtered: a release candidate.


def test_fetch_prepends_current_when_zero_patch():
    # Relevant right after a major/minor bump, before any patch release: current
    # version has `.0` patch, which the tag filter drops. Registry must still
    # expose it as `VersionAlias.CURRENT` pointing at the local build, so the
    # suite can run against the in-development version itself.
    registry = make_registry(
        current_version="26.0.0",
        available_tags=["26.0.0", "25.5.1"],
    )

    versions = [str(e.version) for e in registry.executables]
    assert versions[0] == "26.0.0"  # Prepended current, first.

    current_entry = registry.get(Version("26.0.0"), resolve=False)
    assert current_entry is not None and current_entry.alias == VersionAlias.CURRENT
    assert current_entry.path == LOCAL_BUILD


def test_fetch_sorted_descending():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.4.1", "25.5.1", "25.3.7", "25.5.2"],
    )

    versions = [e.version for e in registry.executables]
    assert versions == sorted(versions, reverse=True)


def test_populate_picks_highest_patch_per_minor():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1", "25.4.9", "25.3.7"],
    )

    current = registry.get(VersionAlias.CURRENT, resolve=False)
    assert current is not None and current.version == Version("25.5.3")

    previous_minor = registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False)
    assert previous_minor is not None and previous_minor.version == Version("25.4.9")

    beforelast_minor = registry.get(VersionAlias.BEFORELAST_MINOR, resolve=False)
    assert beforelast_minor is not None and beforelast_minor.version == Version("25.3.7")


def test_populate_previous_minor_none_when_only_zero_patch_tagged():
    registry = make_registry(
        current_version="25.5.1",
        available_tags=["25.5.1", "25.4.0"],
    )

    assert registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False) is None


def test_populate_previous_minor_resolves_to_nonzero_patch():
    registry = make_registry(
        current_version="25.5.1",
        available_tags=["25.5.1", "25.4.0", "25.4.1"],
    )
    previous_minor = registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False)
    assert previous_minor is not None and previous_minor.version == Version("25.4.1")


def test_populate_major_aliases():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "24.9.1", "23.4.1"],
    )

    previous_major = registry.get(VersionAlias.PREVIOUS_MAJOR, resolve=False)
    assert previous_major is not None and previous_major.version == Version("24.9.1")

    beforelast_major = registry.get(VersionAlias.BEFORELAST_MAJOR, resolve=False)
    assert beforelast_major is not None and beforelast_major.version == Version("23.4.1")


def test_populate_missing_aliases_are_none():
    registry = make_registry(
        current_version="25.0.1",
        available_tags=["25.0.1"],
    )

    assert registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False) is None
    assert registry.get(VersionAlias.BEFORELAST_MINOR, resolve=False) is None
    assert registry.get(VersionAlias.PREVIOUS_MAJOR, resolve=False) is None
    assert registry.get(VersionAlias.BEFORELAST_MAJOR, resolve=False) is None


def test_get_by_version_and_alias():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
    )

    by_version = registry.get(Version("25.4.1"), resolve=False)
    by_alias = registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False)
    assert by_version is not None and by_alias is not None
    assert by_version.version == by_alias.version


def test_get_or_skip_skips():
    registry = make_registry(
        current_version="25.0.1",
        available_tags=["25.0.1"],
    )

    with pytest.raises(pytest.skip.Exception):
        registry.get_or_skip(VersionAlias.PREVIOUS_MAJOR, resolve=False)


def test_resolve_uses_path_lookup():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
        executable_list={"picodata-25.4.1"},
    )

    executable = registry.get(VersionAlias.PREVIOUS_MINOR)
    assert executable is not None and executable.resolved
    assert executable.version == Version("25.4.1")
    assert executable.path == Path("/fake/bin/picodata-25.4.1")


def test_resolve_fallback_to_previous_patch():
    # Relevant when the latest tagged patch is not yet published as a binary
    # (e.g. `25.4.9` is tagged but only `picodata-25.4.8` is on $PATH). Resolve
    # must fall back to `patch - 1` so rolling upgrade tests, which only care
    # about minor/major paths, keep running without waiting for the artifact.
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.9"],
        executable_list={"picodata-25.4.8"},
    )
    executable = registry.get(VersionAlias.PREVIOUS_MINOR)
    assert executable is not None and executable.resolved
    assert executable.version == Version("25.4.8")
    assert executable.path == Path("/fake/bin/picodata-25.4.8")


def test_resolve_no_fallback_when_patch_inappropriate():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
        executable_list={"picodata-25.4.0"},
        err_on_missing_binaries=True,
    )

    entry = registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False)
    assert entry is not None

    with pytest.raises(ValueError):
        entry.resolve()


@pytest.mark.parametrize(
    "err_on_missing_binaries, expected_exception",
    [
        (True, ValueError),  # Outside of CI.
        (False, pytest.skip.Exception),  # At the CI.
    ],
    ids=["raises_in_ci", "skips_outside_ci"],
)
def test_resolve_missing(err_on_missing_binaries, expected_exception):
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.5"],
        err_on_missing_binaries=err_on_missing_binaries,
    )

    entry = registry.get(VersionAlias.PREVIOUS_MINOR, resolve=False)
    assert entry is not None

    with pytest.raises(expected_exception):
        entry.resolve()


def test_current_alias_uses_local_build():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
    )
    current = registry.get(VersionAlias.CURRENT)  # Must not need $PATH.
    assert current is not None and current.resolved
    assert current.version == Version("25.5.3")
    assert current.path == LOCAL_BUILD


def test_next_version_skips_patches():
    registry = make_registry(
        current_version="25.6.1",
        available_tags=["25.6.1", "25.5.1", "25.5.2", "25.4.2", "25.4.1"],
    )

    # Iterates ascending; first non-patch bump wins (25.5.1, not 25.5.2).
    assert registry.next_version(Version("25.4.1")) == Version("25.5.1")


def test_next_version_skips_test_when_minor_is_missing():
    registry = make_registry(
        current_version="26.3.0",
        available_tags=["26.3.0", "26.2.0", "26.1.1"],
    )

    with pytest.raises(
        pytest.skip.Exception,
        match=r"Cannot test upgrade from 26\.1\.1 to 26\.3\.0: the next minor version is not available",
    ):
        registry.next_version(Version("26.1.1"))


def test_next_version_allows_gap_during_collection():
    registry = make_registry(
        current_version="26.3.0",
        available_tags=["26.3.0", "26.2.0", "26.1.1"],
    )

    assert registry.next_version(Version("26.1.1"), skip_on_gap=False) == Version("26.3.0")


def test_next_version_returns_current_when_no_newer():
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
    )

    assert registry.next_version(Version("25.5.3")) == Version("25.5.3")


def test_next_version_unknown_after_returns_current():
    # Relevant when a caller asks for the version following one the registry
    # does not know about (foreign tag, filtered-out `.0` patch, or a stale
    # pin). Instead of raising, `next_version` degrades to current so upgrade
    # iteration has a well-defined binary to work with.
    registry = make_registry(
        current_version="25.5.3",
        available_tags=["25.5.3", "25.4.1"],
    )

    assert registry.next_version(Version("99.99.99")) == Version("25.5.3")
