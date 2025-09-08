from __future__ import annotations

import enum


@enum.unique
class Version(enum.Enum):
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
        return self in [Version.BEFORELAST_MINOR, Version.BEFORELAST_MAJOR]

    @property
    def is_previous(self) -> bool:
        return self in [Version.PREVIOUS_MINOR, Version.PREVIOUS_MAJOR]

    @property
    def is_current(self) -> bool:
        return self == Version.CURRENT

    @property
    def is_major(self) -> bool:
        return self in [Version.PREVIOUS_MAJOR, Version.BEFORELAST_MAJOR]

    @property
    def is_minor(self) -> bool:
        return self in [Version.PREVIOUS_MINOR, Version.BEFORELAST_MINOR]

    def is_mismatch(self, another: Version) -> bool:
        straight = self.is_current and another.is_beforelast
        reversed = another.is_current and self.is_beforelast
        return straight or reversed
