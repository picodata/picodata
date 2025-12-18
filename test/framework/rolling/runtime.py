from __future__ import annotations

from dataclasses import dataclass
from packaging.version import Version as AbsoluteVersion
from pathlib import Path
from typing import Optional

import os
import shutil

from framework.rolling.version import RelativeVersion

import pytest


@dataclass(init=False)
class Runtime:
    absolute_version: AbsoluteVersion
    relative_version: RelativeVersion
    runner_entity: Optional[Path]

    def __init__(
        self,
        absolute_version: AbsoluteVersion,
        relative_version: RelativeVersion,
        runner_entity: Optional[Path] = None,
    ) -> None:
        if absolute_version.micro == 0 and relative_version != RelativeVersion.CURRENT:
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
        self.runner_entity = runner_entity

    @classmethod
    def current(cls) -> Runtime:
        from conftest import binary_path

        return binary_path()

    @property
    def is_resolved(self) -> bool:
        return self.runner_entity is not None

    @property
    def command(self) -> str:
        return str(self.runner_entity)

    def resolve(self) -> None:
        if not self.is_resolved:
            executable_name = f"picodata-{self.absolute_version}"

            executable_path = shutil.which(executable_name)
            if executable_path is not None:
                self.runner_entity = Path(executable_path)
                return

            explain = f"which is needed to test against {self.relative_version.name.lower().replace('_', ' ')}"
            help = "Consider installing it from repository, or building it manually"
            error = f"'{executable_name}' not found in PATH, {explain}. {help}."
            if os.environ.get("CI") is not None:
                raise ValueError(error)
            else:
                pytest.skip(error)
