from __future__ import annotations

from dataclasses import dataclass
from packaging.version import Version as AbsoluteVersion
from pathlib import Path
from typing import Optional

import os
import shutil

from framework.rolling.version import RelativeVersion

import pytest


@dataclass
class Runtime:
    absolute_version: AbsoluteVersion
    relative_version: RelativeVersion
    runner_entity: Optional[Path] = None

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
