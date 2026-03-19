import filecmp
import os
import shutil
import sys

from dataclasses import dataclass
from framework.log import log
from pathlib import Path
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


@dataclass(frozen=True)
class ExpectedError:
    """
    Declares what kind of error an entity is expected to produce.
    """

    log_pattern: str | None = None
    """
    A substring expected in instance logs.
    """

    exception_type: type[Exception] | None = None
    """
    The type of exception expected to be raised.
    """

    exception_match: str | None = None
    """
    A substring expected in the exception message.
    """

    def __post_init__(self) -> None:
        all_fields = self.__dict__.values()
        set_fields = filter(None, all_fields)
        set_count = len(list(set_fields))
        if set_count != 1:
            error = "only one of the fields must be set"
            hint = f"got {set_count}: {repr(self)}"
            raise ValueError(f"{error}, {hint}")

        hint = f"expected error state: {repr(self)}"
        if self.exception_match is not None and len(self.exception_match) == 0:
            error = "`self.exception_match` is set, but is empty"
            raise ValueError(f"{error}, {hint}")
        elif self.log_pattern is not None and len(self.log_pattern) == 0:
            error = "`self.log_pattern` is set, but is empty"
            raise ValueError(f"{error}, {hint}")

    def matches_exception(self, exc: Exception) -> bool:
        if self.exception_type is not None:
            return isinstance(exc, self.exception_type)
        if self.exception_match is not None:
            return self.exception_match in str(exc)
        # `log_pattern` is not matchable against an exception.
        return False

    @property
    def expects_log(self) -> bool:
        return self.log_pattern is not None

    @property
    def description(self) -> str:
        if self.log_pattern is not None:
            return f"log pattern '{self.log_pattern}'"
        if self.exception_type is not None:
            return f"exception type {self.exception_type.__name__}"
        if self.exception_match is not None:
            return f"exception containing '{self.exception_match}'"
        return "unknown"
