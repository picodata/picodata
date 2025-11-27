from __future__ import annotations
from packaging.version import InvalidVersion, Version

import enum


@enum.unique
class RelativeVersion(enum.Enum):
    """
    Represents the relative age of a version compared to the current version.
    Used to select appropriate versions for compatibility testing in rolling
    upgrade scenarios.
    """

    @staticmethod
    def _generate_next_value_(
        name,
        start,
        count,
        last_values,
    ):
        _ = name
        _ = start
        _ = last_values
        return count

    CURRENT = enum.auto()

    PREVIOUS_MINOR = enum.auto()
    BEFORELAST_MINOR = enum.auto()

    PREVIOUS_MAJOR = enum.auto()
    BEFORELAST_MAJOR = enum.auto()

    @property
    def is_beforelast(self) -> bool:
        return self in [RelativeVersion.BEFORELAST_MINOR, RelativeVersion.BEFORELAST_MAJOR]

    @property
    def is_previous(self) -> bool:
        return self in [RelativeVersion.PREVIOUS_MINOR, RelativeVersion.PREVIOUS_MAJOR]

    @property
    def is_current(self) -> bool:
        return self == RelativeVersion.CURRENT

    @property
    def is_major(self) -> bool:
        return self in [RelativeVersion.PREVIOUS_MAJOR, RelativeVersion.BEFORELAST_MAJOR]

    @property
    def is_minor(self) -> bool:
        return self in [RelativeVersion.PREVIOUS_MINOR, RelativeVersion.BEFORELAST_MINOR]

    def is_mismatch(self, another: RelativeVersion) -> bool:
        straight = self.is_current and another.is_beforelast
        reversed = another.is_current and self.is_beforelast
        return straight or reversed


def parse_picodata_version(v: str) -> Version:
    try:
        return Version(v)
    except InvalidVersion as e:
        invalid_version_exception = e
        pass

    try:
        version, hash = v.rsplit("-", maxsplit=1)
        _ = hash
        return Version(version)
    except ValueError:
        # If that didn't work, just raise the original exception
        raise invalid_version_exception from invalid_version_exception
