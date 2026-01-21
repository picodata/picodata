from framework.util.path import project_root_path
from git import Git
from git import Repo
from packaging.version import Version


def project_git_instance() -> Git:
    return Git(project_root_path())


def project_repo_instance() -> Repo:
    return Repo(project_root_path())


def project_git_version() -> Version:
    git_describe = project_git_instance().describe()
    version_part, _ = git_describe.split("-", maxsplit=1)
    return Version(version_part)
