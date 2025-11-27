from __future__ import annotations

from typing import List, Optional

from framework.rolling.runtime import Runtime
from framework.rolling.version import RelativeVersion

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
        self.entries = [None] * len(RelativeVersion)
        self.populate()

    def _fetch(self, version: RelativeVersion) -> None:
        from conftest import Repository

        repository = Repository()

        match version:
            case RelativeVersion.CURRENT:
                self.entries[version.value] = Runtime.current()
            case RelativeVersion.PREVIOUS_MINOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major == current.major and current.minor - tag.minor == 1:
                        self.entries[version.value] = Runtime(tag, version)
            case RelativeVersion.BEFORELAST_MINOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major == current.major and current.minor - tag.minor == 2:
                        self.entries[version.value] = Runtime(tag, version)
            case RelativeVersion.PREVIOUS_MAJOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major - current.major == 1:
                        self.entries[version.value] = Runtime(tag, version)
            case RelativeVersion.BEFORELAST_MAJOR:
                current = repository.current_version()
                for tag in repository.rolling_versions():
                    if tag.major - current.major == 2:
                        self.entries[version.value] = Runtime(tag, version)

    def populate(self) -> None:
        self._fetch(RelativeVersion.CURRENT)
        self._fetch(RelativeVersion.PREVIOUS_MINOR)
        self._fetch(RelativeVersion.BEFORELAST_MINOR)
        self._fetch(RelativeVersion.PREVIOUS_MAJOR)
        self._fetch(RelativeVersion.BEFORELAST_MAJOR)

        for entry in self.entries:
            if entry is not None:
                entry.resolve()

    def get(self, version: RelativeVersion) -> Runtime:
        requested_entry = self.entries[version.value]
        if not requested_entry:
            pytest.skip(f"no need to test against {version.name.lower()} version")

        if version in (RelativeVersion.PREVIOUS_MINOR, RelativeVersion.BEFORELAST_MINOR):
            current_entry = self.entries[RelativeVersion.CURRENT.value]
            assert current_entry

            not_the_same_major = current_entry.absolute_version.major != requested_entry.absolute_version.major
            if not_the_same_major:
                pytest.skip(
                    f"{version.name.lower()} is actually {RelativeVersion.PREVIOUS_MAJOR} or {RelativeVersion.BEFORELAST_MAJOR}"
                )

        return requested_entry
