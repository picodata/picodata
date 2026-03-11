from __future__ import annotations

import os
import pytest
import shutil

from dataclasses import dataclass
from framework.util.build import picodata_executable_path
from framework.util.git import project_git_version
from framework.rolling.version import VersionAlias
from packaging.version import Version as AbsoluteVersion
from pathlib import Path
from typing import Optional


@dataclass(init=False)
class Runtime:
    absolute_version: AbsoluteVersion
    relative_version: VersionAlias
    executable_path: Optional[Path]

    def __init__(
        self,
        absolute_version: AbsoluteVersion,
        relative_version: VersionAlias,
        executable_path: Optional[Path] = None,
    ) -> None:
        if absolute_version.micro == 0 and relative_version != VersionAlias.CURRENT:
            # NOTE: this problem occurs when we branched new minor, but didnt make a release yet.
            # So e g we have 25.5.0 tag from previous series without 25.5.1 tag and 25.6.0 tag in master.
            # For 25.6.0 we cant really find previous minor, which would've been 25.5.1 but 25.5.1 is not tagged yet.
            # We attempted a solution with rc tag e g 25.5.1-rc1 but it doesnt work with our packaging solution -
            # packpack fails to parse such versions.
            reason = f"Cannot test against {relative_version.name} version \
                ({absolute_version}) as it has a zero component. \
                We dont build packages for such versions. \
                The test will work once non-zero {absolute_version} release is tagged."
            ValueError(reason)

        self.absolute_version = absolute_version
        self.relative_version = relative_version
        self.executable_path = executable_path

    @classmethod
    def current(cls) -> Runtime:
        absolute_version = project_git_version()
        relative_version = VersionAlias.CURRENT
        executable_path = picodata_executable_path()
        return Runtime(absolute_version, relative_version, executable_path)

    @property
    def is_resolved(self) -> bool:
        return self.executable_path is not None

    @property
    def command(self) -> str:
        return str(self.executable_path)

    def resolve(self) -> None:
        if not self.is_resolved:
            executable_name = f"picodata-{self.absolute_version}"

            executable_path = shutil.which(executable_name)
            if executable_path is not None:
                self.executable_path = Path(executable_path)
                return

            explain = f"which is needed to test against {self.relative_version.name}"
            help = "Consider installing it from repository, or building it manually"
            error = f"'{executable_name}' not found in PATH, {explain}. {help}."
            if os.environ.get("CI") is not None:
                raise ValueError(error)
            else:
                pytest.skip(error)
