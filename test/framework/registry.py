from framework.util.build import Executable
from framework.util.build import picodata_executable_path
from framework.util.version import ExecutableVersion
from framework.util.version import parse_version_opt
from framework.util.version import VersionAlias
from framework.util.git import project_repo_instance
from framework.util.git import project_git_version
from framework.util import should_err_on_missing_binaries
from packaging.version import Version
from pathlib import Path
from typing import Callable, List

import functools
import pytest
import shutil


class Registry:
    """
    Maintains a collection of project executables derived from git tags, used
    by the rolling upgrade test suite to locate binaries of previous versions
    and orchestrate mixed-version clusters.

    The class is sans-I/O: it operates purely on the snapshot of facts passed
    to `__init__`. All environment-dependent gathering (git tags, current
    version, $PATH lookup, CI detection, local build path) is performed by
    the caller and injected here. The factory `get_or_make_registry` is the
    single seam where real I/O is wired in.
    """

    executables: List[Executable]
    """
    A sorted list of available project executables, populated during
    construction from the injected tag list (plus the current local build if
    its patch is zero). Stored as `Executable` objects which hold version
    metadata and resolution logic.
    """

    current_version: Version
    """
    Current version of the project, as supplied by the caller (typically
    `framework.util.git.project_git_version`).
    """

    def __init__(
        self,
        current_version: Version,
        tags: List[Version],
        local_build_path: Path,
        binary_lookup: Callable[[Version], Path | None],
        err_on_missing_binaries: bool,
    ):
        """
        Build a registry from a static snapshot of facts.

        Parameters:
            current_version: version of the locally built binary
                             (typically `git describe`).
            tags: every release tag known to the project, parsed as `Version`.
                  Filtering (drop `.0` patches, drop pre-releases) happens here.
            local_build_path: filesystem path to the locally built `picodata`
                              binary, used as the `path` of the CURRENT entry.
            binary_lookup: a function which maps version to its binary path
                           (or `None` if not present). Injected into every
                           non-current `Executable` so `.resolve()`.
            err_on_missing_binaries: when True, `Executable.resolve` raises on
                                     a missing binary; when False, it skips.
        """
        self.current_version = current_version
        self._tags = tags
        self._local_build_path = local_build_path
        self._binary_lookup = binary_lookup
        self._err_on_missing_binaries = err_on_missing_binaries

        self._fetch()
        self._populate()

    def _make_executable(
        self,
        version: Version,
        alias: VersionAlias | None = None,
        path: Path | None = None,
    ) -> Executable:
        return Executable(
            version=version,
            alias=alias,
            path=path,
            binary_lookup=self._binary_lookup,
            err_on_missing_binaries=self._err_on_missing_binaries,
        )

    def _make_current(self) -> Executable:
        return self._make_executable(
            version=self.current_version,
            alias=VersionAlias.CURRENT,
            path=self._local_build_path,
        )

    def _fetch(self):
        def version_filter(version: Version) -> bool:
            return version.micro != 0 and version.pre is None

        valid_tags = filter(version_filter, self._tags)
        sorted_tags = sorted(valid_tags, reverse=True)
        self.executables = [self._make_executable(tag) for tag in sorted_tags]

        # Make a lookup for an executable of current version: if our current
        # version patch component is 0, then it will be ignored by version
        # filter due to our rules with the newly released tags in the project.
        # This will not allow us to test it, so let's add it manually here.
        if self.get(self.current_version, resolve=False) is None:
            self.executables.insert(0, self._make_current())

    def _populate(self) -> None:
        assignment_rules = [
            (
                lambda entry_version: entry_version == self.current_version,
                VersionAlias.CURRENT,
            ),
            (
                lambda entry_version: (
                    entry_version.major == self.current_version.major
                    and self.current_version.minor - entry_version.minor == 1
                ),
                VersionAlias.PREVIOUS_MINOR,
            ),
            (
                lambda entry_version: (
                    entry_version.major == self.current_version.major
                    and self.current_version.minor - entry_version.minor == 2
                ),
                VersionAlias.BEFORELAST_MINOR,
            ),
            (
                lambda entry_version: self.current_version.major - entry_version.major == 1,
                VersionAlias.PREVIOUS_MAJOR,
            ),
            (
                lambda entry_version: self.current_version.major - entry_version.major == 2,
                VersionAlias.BEFORELAST_MAJOR,
            ),
        ]

        # Tracks which version aliases have already been assigned.
        # Multiple candidate versions may satisfy the same alias (e.g.,
        # several patch releases for the previous minor). Without this
        # guard, later iterations would overwrite earlier assignments.
        assigned_aliases: set[VersionAlias] = set()

        for current_index in range(len(self.executables)):
            current_executable = self.executables[current_index]

            if len(assigned_aliases) == len(VersionAlias):
                break

            for condition_rule, condition_version in assignment_rules:
                seen_this_version = condition_version in assigned_aliases
                fits_condition_rule = condition_rule(current_executable.version)

                if fits_condition_rule and not seen_this_version:
                    assigned_aliases.add(condition_version)
                    current_executable.alias = condition_version

                    # We ensure that the `VersionAlias.CURRENT` always
                    # points to the locally built executable rather than a
                    # potentially matched git tag. Replacing the entry in the
                    # list allows us to keep the list-based lookup structure
                    # intact without manually mutating complex internal fields
                    # of the `Executable` object.
                    if condition_version == VersionAlias.CURRENT:
                        self.executables[current_index] = self._make_current()

                    break  # Only one alias per entry.

    def get(
        self,
        version: ExecutableVersion,
        resolve: bool = True,
    ) -> Executable | None:
        """
        Retrieves an executable from the registry by its version or alias.
        Executable resolution is performed lazily during this call rather
        than at registry initialization. Parameter `resolve` controls whether
        to resolve the executable's binary path on the system, otherwise returns
        the `Executable` entry as-is without attempting to locate its binary in $PATH.
        """
        for executable in self.executables:
            version_condition = isinstance(version, Version) and executable.version != version
            alias_condition = isinstance(version, VersionAlias) and executable.alias != version
            if version_condition or alias_condition:
                continue

            if not resolve:
                return executable

            executable.resolve()
            return executable

        return None

    def get_or_skip(
        self,
        version: ExecutableVersion,
        resolve: bool = True,
    ) -> Executable:
        """
        Same as `Registry.get`, but instead of returning `None`, it calls `pytest.skip`.
        """
        executable = self.get(version, resolve)
        if executable is None:
            reason = f"No need to test against {version}"
            hint = "there is no such version released"
            pytest.skip(f"{reason}: {hint}")
        return executable

    def next_version(self, after: Version, *, skip_on_gap: bool = True) -> Version:
        """
        Finds the next significant version (minor or major)
        after `after`. This method ignores patch updates.

        It looks for the next immediate minor version. If no minor
        version exists, it looks for the next major version. If no
        major version exists, it returns current version by Git.

        `skip_on_gap` parameter skips the current test when the next available
        version jumps over an intermediate minor. Calls evaluated in marker
        decorators must disable it because they run during collection, where
        `pytest.skip` would affect the entire module instead of a single test.
        The test body should then call this method with the default.
        """
        executable = self.get(after, resolve=False)
        if executable is None:
            return self.current_version

        base_version = executable.version

        # XXX: Works properly only because the `self.executables` list is
        # sorted descending, and releases are not skipped, thus sequential.
        for executable in reversed(self.executables):
            candidate_version = executable.version

            if candidate_version <= base_version:
                continue

            # We want next minor or major, which means we
            # simply skip if the change is only a patch.
            if candidate_version.major == base_version.major and candidate_version.minor == base_version.minor:
                continue

            if (
                skip_on_gap
                and candidate_version.major == base_version.major
                and candidate_version.minor > base_version.minor + 1
            ):
                pytest.skip(
                    f"Cannot test upgrade from {base_version} to {candidate_version}: "
                    "the next minor version is not available"
                )

            return candidate_version

        return self.current_version


def _default_binary_lookup(version: Version) -> Path | None:
    """
    System (real) `binary_lookup`: resolves `picodata-<version>` via $PATH.
    """
    path = shutil.which(f"picodata-{version}")
    return Path(path) if path is not None else None


@functools.cache
def get_or_make_registry() -> Registry:
    """
    Single point of impurity: collects facts from the environment and
    constructs a `Registry`. All callers from integrational test suite should
    go through this factory; harness tests should construct `Registry` directly.
    """
    raw_tags = map(str, project_repo_instance().tags)
    parsed_tags = [v for v in map(parse_version_opt, raw_tags) if v is not None]

    return Registry(
        current_version=project_git_version(),
        tags=parsed_tags,
        local_build_path=picodata_executable_path(),
        binary_lookup=_default_binary_lookup,
        err_on_missing_binaries=should_err_on_missing_binaries(),
    )
