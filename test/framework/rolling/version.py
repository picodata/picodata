import enum


@enum.unique
class VersionAlias(enum.Enum):
    """
    Represents the relative age of a version compared to the current version.
    Used to select appropriate versions for compatibility testing in rolling
    upgrade scenarios.
    """

    # NOTE: override `enum.auto()` value generation to assign zero-based, contiguous
    # integer values (0, 1, 2, so on). This is required so enum values can be used
    # as stable indices (e.g., into lists or arrays) without an off-by-one offset.
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

    def __str__(self):
        return self.name.lower().replace("_", " ")

    # The major version two steps before the current version (e.g., if current is 3.x, this is 1.x).
    # Used to test scenarios where upgrades must be sequential and cannot skip major versions.
    BEFORELAST_MAJOR = enum.auto()

    # The major version immediately before the current version (e.g., if current is 3.x, this is 2.x).
    # Used to test compatibility and upgrade paths between consecutive major versions.
    PREVIOUS_MAJOR = enum.auto()

    # The minor version two steps before the current version (e.g., if current is 2.2, this is 2.0).
    # Used to test sequential minor version upgrades and to verify that skipping minor versions fails.
    BEFORELAST_MINOR = enum.auto()

    # The minor version immediately before the current version (e.g., if current is 2.2, this is 2.1).
    # Used to test direct upgrades from the previous minor version and rollback scenarios.
    PREVIOUS_MINOR = enum.auto()

    # The current and/or latest version being tested.
    # Represents the target version for upgrade tests and the baseline for downgrade rejection tests.
    CURRENT = enum.auto()
