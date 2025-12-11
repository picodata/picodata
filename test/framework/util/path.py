import subprocess

from pathlib import Path


def project_root_path() -> Path:
    git_command = ["git", "rev-parse", "--show-toplevel"]
    git_output = subprocess.check_output(git_command, text=True)
    return Path(git_output.strip())


def project_tests_path() -> Path:
    tests_path = project_root_path() / "test"

    error = "tests directory of the project must exist"
    assert tests_path.exists(), error

    return tests_path
