#!/usr/bin/env python3

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

"""Update the dependency lock files located at 'requirements.txt' and
'requirements-dev.txt'.
"""

import os
import subprocess
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import tomli
import tomli_w
from script_utils import cli

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()

PYPROJECT_TOML_PATH = REPO_ROOT_DIR / "pyproject.toml"
DEV_COMMON_DEPS_PATH = REPO_ROOT_DIR / "requirements-dev-common.in"
DEV_DEPS_PATH = REPO_ROOT_DIR / "requirements-dev.in"
OUTPUT_LOCK_PATH = REPO_ROOT_DIR / "requirements.txt"
OUTPUT_DEV_LOCK_PATH = REPO_ROOT_DIR / "requirements-dev.txt"


def exclude_from_dependency_list(*, package_name: str, dependencies: list) -> list:
    """Exclude the specified package from the provided dependency list."""

    return [
        dependency
        for dependency in dependencies
        if not dependency.startswith(package_name)
    ]


def remove_self_dependencies(pyproject: dict) -> dict:
    """Filter out self dependencies (dependencies of the package on it self) from the
    dependencies and optional-depenendencies in the provided pyproject metadata."""

    if not "project" in pyproject:
        return pyproject

    modified_pyproject = deepcopy(pyproject)

    project_metadata = modified_pyproject["project"]

    package_name = project_metadata.get("name")

    if not package_name:
        raise ValueError("The provided project metadata does not contain a name.")

    if project_metadata["dependencies"]:
        project_metadata["dependencies"] = exclude_from_dependency_list(
            package_name=package_name, dependencies=project_metadata["dependencies"]
        )

    if project_metadata["optional-dependencies"]:
        for group in project_metadata["optional-dependencies"]:
            project_metadata["optional-dependencies"][
                group
            ] = exclude_from_dependency_list(
                package_name=package_name,
                dependencies=project_metadata["optional-dependencies"][group],
            )

    return modified_pyproject


def compile_lock_file(sources: list[Path], output: Path) -> None:
    """From the specified sources compile a lock file using pip-compile from pip-tools
    and write it to the specified output location.
    """

    command = [
        "pip-compile",
        "--rebuild",
        "--generate-hashes",
        "--annotate",
        "--all-extras",
        "--output-file",
        str(output.absolute()),
    ] + [str(source.absolute()) for source in sources]

    with subprocess.Popen(
        args=command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ) as process:
        process.communicate()
        if process.wait() != 0:
            stdout = process.stdout
            log = stdout.read().decode("utf-8") if stdout else "no log awailable."
            raise RuntimeError(f"Failed to compile lock file:\n{log}")


def main():
    """Update the dependency lock files located at 'requirements.txt' and
    'requirements-dev.txt'.

    For the 'requirements.txt' only the package with its dependencies being defined in
    the 'pyproject.toml' are considered. Thereby, all recursive dependencies of the
    package on itself are removed.

    For the 'requirements-dev.txt', in addition to the filtered 'pyproject.toml' the
    'requirements-dev.in' is considered.
    """

    with open(PYPROJECT_TOML_PATH, "rb") as pyproject_toml:
        pyproject = tomli.load(pyproject_toml)

    modified_pyproject = remove_self_dependencies(pyproject)

    with TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)

        modified_pyproject_path = Path(temp_dir) / "pyproject.toml"
        with open(modified_pyproject_path, "wb") as modified_pyproject_toml:
            tomli_w.dump(modified_pyproject, modified_pyproject_toml)

        compile_lock_file(sources=[modified_pyproject_path], output=OUTPUT_LOCK_PATH)
        compile_lock_file(
            sources=[modified_pyproject_path, DEV_COMMON_DEPS_PATH],
            output=OUTPUT_DEV_LOCK_PATH,
        )

    cli.echo_success(
        f"Successfully updated lock files at '{OUTPUT_LOCK_PATH}' and"
        + f" '{OUTPUT_DEV_LOCK_PATH}'."
    )


if __name__ == "__main__":
    cli.run(main)
