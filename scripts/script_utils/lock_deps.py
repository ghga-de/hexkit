# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Provides a function to get all dependencies from the lock file"""
import re
from pathlib import Path
from typing import Optional

from packaging.requirements import Requirement


def get_lock_file_deps(
    lock_file_path: Path,
    exclude: Optional[set[str]] = None,
) -> list[Requirement]:
    """Inspect the lock file to get the dependencies.

    Return a list of Requirements objects that contain the dependency info.
    """
    dependency_pattern = re.compile(r"([^=\s]+==[^\s]*?)\s")

    # Get the set of dependencies from the provided lock file
    with open(lock_file_path, encoding="utf-8") as lock_file:
        lines = lock_file.readlines()

    dependencies: list[Requirement] = []
    for line in lines:
        if match := re.match(dependency_pattern, line):
            dependency_string = match.group(1)
            requirement = Requirement(dependency_string)
            if not exclude or requirement.name not in exclude:
                dependencies.append(requirement)

    return dependencies
