from framework.util.build import Executable
from framework.util.version import ExecutableVersion
from framework.util.version import parse_version_opt
from framework.util.version import VersionAlias
from framework.util.git import project_repo_instance
from framework.util.git import project_git_version
from packaging.version import Version
from typing import List

import pytest


class Registry:
    """
    This entity maintains a collection of project executables derived from git tags.
    It is primarily used by the rolling upgrade test suite to dynamically locate and
    retrieve binaries of previous versions. This allows the framework to orchestrate
    clusters with mixed versions to verify backward compatibility and upgrade paths.
    """

    executables: List[Executable]
    """
    A sorted list of available project executables. This list is populated during
    initialization by scanning git tags and includes the current local build.
    Entries are stored as `Executable` objects which hold version metadata and resolution logic.
    """

    current_version: Version
    """
    Current version of the project, taken from `git describe`.
    This is essentially `framework.utils.project_git_version`
    stored as a field just to avoid calling it every time.
    """

    def __init__(self):
        self.current_version = project_git_version()
        self._fetch()
        self._populate()

    def _fetch(self):
        def version_filter(version: Version | None) -> bool:
            return version is not None and version.micro != 0 and version.pre is None

        string_tags = map(str, project_repo_instance().tags)
        converted_tags = list(map(parse_version_opt, string_tags))
        valid_tags = filter(version_filter, converted_tags)
        sorted_tags = sorted(valid_tags, reverse=True)
        self.executables = [Executable(tag) for tag in sorted_tags]

        # Make a lookup for an executable of current version: if our current
        # version patch component is 0, then it will be ignored by version
        # filter due to our rules with the newly released tags in the project.
        # This will not allow us to test it, so let's add it manually here.
        if self.get(self.current_version) is None:
            current_executable = Executable.current()
            self.executables.insert(0, current_executable)

    def _populate(self) -> None:
        assignment_rules = [
            (
                lambda entry_version: entry_version == self.current_version,
                VersionAlias.CURRENT,
            ),
            (
                lambda entry_version: entry_version.major == self.current_version.major
                and self.current_version.minor - entry_version.minor == 1,
                VersionAlias.PREVIOUS_MINOR,
            ),
            (
                lambda entry_version: entry_version.major == self.current_version.major
                and self.current_version.minor - entry_version.minor == 2,
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
                        self.executables[current_index] = Executable.current()

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

    def next_version(self, after: Version) -> Version:
        """
        Finds the next significant version (minor or major)
        after `after`. This method ignores patch updates.

        It looks for the next immediate minor version. If no minor
        version exists, it looks for the next major version. If no
        major version exists, it returns current version by Git.
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

            # Since the list is sequential, the first non-patch version
            # we encounter is the next minor (or major if no minor exists).
            return candidate_version

        return self.current_version
