#!/usr/bin/env python3

# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Extracts the package name from pyproject.toml"""

from pathlib import Path

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
PYPROJECT_TOML_PATH = REPO_ROOT_DIR / "pyproject.toml"
NAME_PREFIX = "name = "


def get_package_name() -> str:
    """Extracts the package name"""

    with open(PYPROJECT_TOML_PATH, encoding="utf8") as pyproject_toml:
        for line in pyproject_toml.readlines():
            line_stripped = line.strip()
            if line_stripped.startswith(NAME_PREFIX):
                package_name = line_stripped[len(NAME_PREFIX) :]
                return package_name.strip('"')
        raise RuntimeError("Could not find package name.")


def run():
    """Run this script."""
    package_name = get_package_name()
    print(package_name)


if __name__ == "__main__":
    run()
