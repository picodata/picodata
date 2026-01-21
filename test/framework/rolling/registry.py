from __future__ import annotations

from typing import List, Optional

from framework.rolling.runtime import Runtime
from framework.rolling.version import VersionAlias

import pytest


class Registry:
    entries: List[Optional[Runtime]]

    def __repr__(self) -> str:
        result = "{"
        for entry in self.entries:
            if entry:
                result += f"\n\t{str(entry)},"
        result += "\n}"
        return result

    def __str__(self) -> str:
        return self.__repr__()

    def __init__(self) -> None:
        self.entries = [None] * len(VersionAlias)
        self.populate()

    def _fetch(self, version: VersionAlias) -> None:
        from conftest import Repository

        repository = Repository()

        match version:
            case VersionAlias.CURRENT:
                self.entries[version.value] = Runtime.current()
            case VersionAlias.PREVIOUS_MINOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major == current.major and current.minor - tag.minor == 1:
                        self.entries[version.value] = Runtime(tag, version)
            case VersionAlias.BEFORELAST_MINOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major == current.major and current.minor - tag.minor == 2:
                        self.entries[version.value] = Runtime(tag, version)
            case VersionAlias.PREVIOUS_MAJOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major - current.major == 1:
                        self.entries[version.value] = Runtime(tag, version)
            case VersionAlias.BEFORELAST_MAJOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major - current.major == 2:
                        self.entries[version.value] = Runtime(tag, version)

    def populate(self) -> None:
        self._fetch(VersionAlias.CURRENT)
        self._fetch(VersionAlias.PREVIOUS_MINOR)
        self._fetch(VersionAlias.BEFORELAST_MINOR)
        self._fetch(VersionAlias.PREVIOUS_MAJOR)
        self._fetch(VersionAlias.BEFORELAST_MAJOR)

        for entry in self.entries:
            if entry is not None:
                entry.resolve()

    def get(self, version: VersionAlias) -> Runtime:
        requested_entry = self.entries[version.value]
        if requested_entry is None:
            reason = f"no need to test against {version} version"
            hint = f"there is no {version} version released"
            pytest.skip(f"{reason} because {hint}")
        return requested_entry
