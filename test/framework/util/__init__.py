import filecmp
import os
import shutil
import sys

from framework.log import log
from packaging.version import InvalidVersion
from packaging.version import Version
from pathlib import Path
from typing import Optional
from typing import Union


BASE_HOST = "127.0.0.1"


def eprint(*args, **kwargs):
    """
    Wrapper over builtin `print` which writes to `stderr`.
    """
    print(*args, file=sys.stderr, **kwargs)


# XXX: do not use `sysconfig.get_config_var("SHLIB_SUFFIX")`
# here, as it not always returns the correct value for MacOS.
def dynamic_library_extension() -> str:
    """
    Returns a name suffix for a shared library according to the system.
    """
    match sys.platform:
        case "linux":
            return "so"
        case "darwin":
            return "dylib"

    error = "unsupported platform"
    raise RuntimeError(error)


# TODO: implement a proper plugin installation routine for tests
def copy_plugin_library(
    from_dir: Union[Path, str],
    share_dir: Union[Path, str],
    lib_name: str,
):
    ext = dynamic_library_extension()
    file_name = f"{lib_name}.{ext}"

    src = Path(from_dir) / file_name
    dst = Path(share_dir) / file_name

    if os.path.exists(dst) and filecmp.cmp(src, dst):
        return

    log.info(f"copying plugin from '{src}' to '{dst}'")
    shutil.copyfile(src, dst)


def is_in_ci() -> bool:
    """
    Checks if the current execution is performed in CI.

    NOTE: only checks that $CI env variable is set, not its value.
    """
    return os.environ.get("CI") is not None


def parse_version_opt(version: str) -> Optional[Version]:
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
        raise original_error from original_error


def ask_yes_no(prompt: str, repeat: bool = False) -> bool:
    """
    Ask the user a yes/no question and return the answer as a boolean.

    Arguments:
        * prompt - the question to display to the user.
        * repeat - (True) keep prompting until a valid answer is given,
                   (False) raise ValueError on the first invalid input.
    """
    while True:
        message = f"{prompt} [y/n]: "
        answer = input(message).strip().lower()

        if answer in ("y", "yes"):
            return True

        if answer in ("n", "no"):
            return False

        if not repeat:
            error = "invalid input"
            hint = f"expected: 'y' or 'n', got: {answer}"
            message = f"{error}, {hint}"
            raise ValueError(message)
