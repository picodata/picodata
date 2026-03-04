import enum

from packaging.version import InvalidVersion
from packaging.version import Version


def parse_version_opt(version: str) -> Version | None:
    """
    Wrapper over `parse_version_exc` that returns `None` instead of raising
    exception. Useful when applied to a collection of potential versions via
    higher-order functions like `map`.
    """
    try:
        return parse_version_exc(version)
    except InvalidVersion:
        return None


def parse_version_exc(version: str) -> Version:
    """
    Parse a Picodata version string into a `packaging.version.Version` object.

    Accepts standard PEP 440 versions and Picodata-specific variants that
    include a dash-separated build or commit suffix (e.g. "1.2.3-abcdef"),
    which is ignored for version comparison purposes.

    On failure, raises the original `InvalidVersion` produced when parsing
    the unmodified input.
    """
    # STEP: try to parse as standard PEP 440 version.
    try:
        return Version(version)
    except InvalidVersion as exception:
        original_error = exception

    # STEP: try to parse as Picodata-compatible version.
    # NOTE: only a single trailing dash-suffix is supported.
    try:
        version, _hash = version.rsplit("-", maxsplit=1)
        return Version(version)
    except InvalidVersion:
        raise original_error


def base_version(version: Version) -> Version:
    """
    Removes any additional version metadata such as pre-release
    identifiers (e.g., alpha, beta, rc), post-release markers,
    development markers, local version labels, and build metadata.
    The resulting `Version` is strictly in the form "major.minor.patch".

    This is useful when performing non-strict version comparisons where
    the original version may include extra components that should be
    ignored. For example, instance versions may include post-"release"
    markers such as `25.6.0.post277` after an upgrade operation, but
    only the base version `25.6.0` may be considered for checks.
    """
    major = version.major
    minor = version.minor
    patch = version.micro
    result = f"{major}.{minor}.{patch}"
    return Version(result)


@enum.unique
class VersionAlias(enum.StrEnum):
    """
    Represents the relative age of a version compared to the current version.
    Used to select appropriate versions for compatibility testing in rolling
    upgrade scenarios.
    """

    BEFORELAST_MAJOR = enum.auto()
    """
    The major version two steps before the current version.

    For example, if current version is `3.X.Y`, then this would be `1.X.Y`. In case there are no
    such version available (current version is `<3.X.Y`), version registry will return `None` at
    resolution stage of the executable.

    Used to test scenarios where upgrades must be sequential and cannot skip major versions.
    """

    PREVIOUS_MAJOR = enum.auto()
    """
    The major version one step before the current version.

    For example, if current version is `3.X.Y`, then this would be `2.X.Y`. In case there are no
    such version available (current version is `<2.X.Y`), version registry will return `None` at
    resolution stage of the executable.

    Used to test compatibility and upgrade paths between consecutive major versions.
    """

    BEFORELAST_MINOR = enum.auto()
    """
    The minor version two steps before the current version within the same major version.

    For example, if current version is `2.2.Y`, then this would be `2.0.Y`. In case there are no
    such version available (current version is `<2.2.Y`), version registry will return `None` at
    resolution stage of the executable.

    Used to test scenarios where minor version upgrades must be sequential and cannot skip minor versions.
    """

    PREVIOUS_MINOR = enum.auto()
    """
    The minor version one step before the current version within the same major version.

    For example, if current version is `2.2.Y`, then this would be `2.1.Y`. In case there are no
    such version available (current version is `<2.1.Y`), version registry will return `None` at
    resolution stage of the executable.

    Used to test compatibility and upgrade paths between consecutive minor versions.
    """

    CURRENT = enum.auto()
    """
    The current and/or latest available version.

    For example, if current version is `3.X.Y`, then this refers to `3.X.Y`.
    This version is always expected to be available and cannot resolve to `None`.

    Used as the target version for upgrade tests and as the baseline version for validations.
    """


ExecutableVersion = Version | VersionAlias
"""
A union type representing either a concrete version or a relative version alias.
This type allows specifying a target version in two ways:
1. Explicitly, using a parsed `packaging.version.Version` object.
2. Dynamically, using a `VersionAlias` to select a version relative to the
   current one (e.g., for defining upgrade paths without hardcoding numbers).
In most cases, used only by methods of `framework.util.build.Executable`.
"""
