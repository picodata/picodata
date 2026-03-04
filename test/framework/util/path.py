from pathlib import Path


def project_root_path() -> Path:
    utils_dir = Path(__file__).parent
    framework_dir = utils_dir.parent
    tests_dir = framework_dir.parent
    root_dir = tests_dir.parent
    return root_dir


def project_tests_path() -> Path:
    tests_path = project_root_path() / "test"

    error = "tests directory of the project must exist"
    assert tests_path.exists(), error

    return tests_path
