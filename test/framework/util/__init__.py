import filecmp
import os
import shutil
import sys

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
