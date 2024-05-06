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
#
"""Contains utils for working with dependencies, lock files, etc."""

import tomllib
from copy import deepcopy
from pathlib import Path
from typing import Any

import stringcase


def exclude_from_dependency_list(*, package_name: str, dependencies: list) -> list:
    """Exclude the specified package from the provided dependency list."""

    return [
        dependency
        for dependency in dependencies
        if not dependency.startswith(package_name)
    ]


def remove_self_dependencies(pyproject: dict) -> dict:
    """Filter out self dependencies (dependencies of the package on it self) from the
    dependencies and optional-dependencies in the provided pyproject metadata."""

    if "project" not in pyproject:
        return pyproject

    modified_pyproject = deepcopy(pyproject)

    project_metadata = modified_pyproject["project"]

    package_name = stringcase.spinalcase(project_metadata.get("name"))

    if not package_name:
        raise ValueError("The provided project metadata does not contain a name.")

    if "dependencies" in project_metadata:
        project_metadata["dependencies"] = exclude_from_dependency_list(
            package_name=package_name, dependencies=project_metadata["dependencies"]
        )

    if "optional-dependencies" in project_metadata:
        for group in project_metadata["optional-dependencies"]:
            project_metadata["optional-dependencies"][group] = (
                exclude_from_dependency_list(
                    package_name=package_name,
                    dependencies=project_metadata["optional-dependencies"][group],
                )
            )

    return modified_pyproject


def get_modified_pyproject(pyproject_toml_path: Path) -> dict[str, Any]:
    """Get a copy of pyproject.toml with any self-referencing dependencies removed."""
    with open(pyproject_toml_path, "rb") as pyproject_toml:
        pyproject = tomllib.load(pyproject_toml)

    modified_pyproject = remove_self_dependencies(pyproject)
    return modified_pyproject
