#!/usr/bin/env python3

def path_hack():
    """
    This allows importing project modules located in the 'test/' directory without requiring
    installation or modifying PYTHONPATH externally. The path is appended dynamically based
    on the location of the current script.
    """
    from pathlib import Path
    import sys

    current_file = Path(__file__).resolve()
    tools_directory = current_file.parent
    root_directory = tools_directory.parent
    final_path = root_directory / "test"
    sys.path.insert(0, str(final_path))

# HACK: when running scripts from the 'tools/' directory, the 'test/' directory is not
# automatically in Python's module search path. This dynamically adds it so test modules
# can be imported without requiring installation or manual PYTHONPATH setup.
path_hack()

from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from framework.rolling.registry import Registry
from framework.rolling.version import VersionAlias
from framework.util.path import project_root_path
from framework.util import ask_yes_no
from packaging.version import Version
from pathlib import Path

import shutil
import subprocess


def is_working_tree_dirty() -> bool:
    """
    Checks if the Git working tree has
    uncommitted changes to tracked files.
    """
    # fmt: off
    command = [
        "git", "status",
        "--untracked-files=no",
        "--ignore-submodules=none",
        "--porcelain=v1",
    ]
    # fmt: on
    result = subprocess.check_output(command, text=True)
    return bool(result.strip())


def build_binary(version: Version, place: Path, confirm: bool = False) -> Path:
    # fmt: off
    saved_branch = subprocess.check_output([
        "git", "rev-parse", "--abbrev-ref", "HEAD",
    ], text=True).strip()
    # fmt: on

    subprocess.check_call(["git", "switch", "--detach", f"{version}"])
    subprocess.check_call(["make", "reset-submodules"])

    subprocess.check_call(["make", "build"])

    executable_path = project_root_path() / "target" / "debug" / "picodata"
    output_path = place / f"picodata-{version}"
    shutil.move(executable_path, output_path)

    # NOTE: versions before 25.5.1 contained a submodule that was later removed from the repository.
    # When switching back to the current branch, Git refuses to proceed if the submodule directory
    # still exists on disk, because Git will not auto-delete directories that are (or were) Git
    # repositories. As a result, this submodule must be manually removed to allow branch switching.
    # For versions <= 25.5.1 this removal is mandatory; refusing to do so leaves the working tree
    # in a broken state. See <https://git.picodata.io/core/picodata/-/merge_requests/2399>.
    broken_submodule = project_root_path() / "tarantool"
    if broken_submodule.exists() and version <= Version("25.5.1"):
        submodule_prompt = f"need to remove submodule that is already merged in tree at {broken_submodule}"
        if confirm or ask_yes_no(f"{submodule_prompt}. Removing it?", repeat=False):
            print(f"removing merged submodule at {broken_submodule}")
            shutil.rmtree(broken_submodule)
        else:
            error = f"merged submodule {broken_submodule} is required to be deleted"
            hint = "see 40454d1edf2582577b646bbb22f382a2dc5d12a8 or picodata!2399"
            message = f"{error}, {hint}"
            raise RuntimeError(message)

    subprocess.check_call(["git", "switch", f"{saved_branch}"])
    subprocess.check_call(["make", "reset-submodules"])

    return output_path


def main():
    parser_hint = "rolling upgrade tests require Picodata executables to be available from $PATH"

    arguments_parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
    arguments_parser.description = (
        "Script to build Picodata executables of previous versions.\n"
        f"Note that {parser_hint}"
    )
    arguments_parser.add_argument(
        "output",
        type=Path,
        action="store",
        help=(
            "path to directory where built executables will be placed\n"
            f"note that {parser_hint}"
        ),
    )
    arguments_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="confirm deletion of broken submodule without prompting",
    )

    if is_working_tree_dirty():
        error = "git working tree is dirty"
        hint = "try to commit or stash changes first"
        message = f"{error}, {hint}"
        raise RuntimeError(message)

    program_arguments = arguments_parser.parse_args()
    auto_confirm = program_arguments.yes
    output_path = program_arguments.output

    version_registry = Registry(skip_resolution=True, copy_plugins=False)
    executable_paths = {}

    versions_to_build = [VersionAlias.PREVIOUS_MINOR, VersionAlias.BEFORELAST_MINOR]
    for version_alias in versions_to_build:
        version_runtime = version_registry.get(version_alias)
        assert version_runtime is not None
        version_component = version_runtime.absolute_version

        executable_path = build_binary(version_component, output_path, auto_confirm)
        executable_paths[(version_component, version_alias)] = executable_path

    built_executables = executable_paths.items()
    for (version_component, version_alias), executable_path in built_executables:
        version_string = f"{version_component} ({version_alias})"
        print(f"executable for picodata {version_string} installed to {executable_path}")
    print("note: rolling upgrade tests require Picodata executables to be available from $PATH")


if __name__ == "__main__":
    main()
