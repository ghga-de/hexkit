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
"""Check capped dependencies for newer versions."""
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, NamedTuple

import httpx
from packaging.requirements import Requirement

from script_utils import cli, deps, lock_deps

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
PYPROJECT_TOML_PATH = REPO_ROOT_DIR / "pyproject.toml"
DEV_DEPS_PATH = REPO_ROOT_DIR / "requirements-dev.in"
LOCK_FILE_PATH = REPO_ROOT_DIR / "requirements-dev.txt"


class OutdatedDep(NamedTuple):
    """Encapsulates data of an outdated dependency"""

    name: str
    specified_version: str
    pypi_version: str


def get_main_deps_pyproject(modified_pyproject: dict[str, Any]) -> list[Requirement]:
    """Get a list of the dependencies from pyproject.toml"""

    dependencies: list[str] = []
    if "dependencies" in modified_pyproject["project"]:
        dependencies = modified_pyproject["project"]["dependencies"]

    return [Requirement(dependency) for dependency in dependencies]


def get_optional_deps_pyproject(
    modified_pyproject: dict[str, Any]
) -> list[Requirement]:
    """Get a list of the optional dependencies from pyproject.toml"""

    dependencies: list[str] = []

    if "optional-dependencies" in modified_pyproject["project"]:
        for optional_dependency_list in modified_pyproject["project"][
            "optional-dependencies"
        ]:
            dependencies.extend(
                modified_pyproject["project"]["optional-dependencies"][
                    optional_dependency_list
                ]
            )

    return [Requirement(dependency) for dependency in dependencies]


def get_deps_dev() -> list[Requirement]:
    """Get a list of raw dependency strings from requirements-dev.in"""
    with open(DEV_DEPS_PATH, encoding="utf-8") as dev_deps:
        dependencies = [
            line
            for line in (line.strip() for line in dev_deps)
            if line  # skip empty lines
            and not line.startswith("#")  # skip comments
            and "requirements-dev-common.in" not in line  # skip inclusion line
        ]

    return [Requirement(dependency) for dependency in dependencies]


def get_version_from_pypi(package_name: str, client: httpx.Client) -> str:
    """Make a call to PyPI to get the version information about `package_name`."""
    try:
        response = client.get(f"https://pypi.org/pypi/{package_name}/json")
        body = response.json()
        version = body["info"]["version"]
    except (httpx.RequestError, KeyError):
        cli.echo_failure(f"Unable to retrieve information for package '{package_name}'")
        sys.exit(1)

    return version


def get_outdated_deps(
    requirements: list[Requirement], strip: bool = False
) -> list[OutdatedDep]:
    """Determine which packages have updates available outside of pinned ranges."""
    outdated: list[OutdatedDep] = []
    with httpx.Client(timeout=10) as client:
        for requirement in requirements:
            pypi_version = get_version_from_pypi(requirement.name, client)

            specified = str(requirement.specifier)

            # Strip the specifier symbols from the front of the string if desired
            if strip:
                specified = specified.lstrip("<=>!~")

            # append package name, specified version, and latest available version
            if not requirement.specifier.contains(pypi_version):
                outdated.append(OutdatedDep(requirement.name, specified, pypi_version))
    outdated.sort()
    return outdated


def print_table(
    rows: Sequence[tuple[str, ...]],
    headers: tuple[str, ...],
    delimiter: str = " | ",
):
    """
    List outdated dependencies in a formatted table.

    Args:
        `outdated`: A sequence of tuples containing strings.
        `headers`: A tuple containing the header strings for the table columns.
    """
    if rows and len(rows[0]) != len(headers):
        raise RuntimeError("Number of headers doesn't match number of columns")

    header_lengths = [len(header) for header in headers]

    # Find the maximum length of each column
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*rows)]

    # Create a row format based on the maximum column widths
    row_format = delimiter.join(
        f"{{:<{max(width, header_len)}}}"
        for width, header_len in zip(col_widths, header_lengths)
    )

    print("  " + row_format.format(*headers))
    for dependency in rows:
        print("  " + row_format.format(*dependency))


def main(transitive: bool = False):
    """Check capped dependencies for newer versions.

    Examine `pyproject.toml` and `requirements-dev.in` for capped dependencies.
    Make a call to PyPI to see if any newer versions exist.

    Use `transitive` to show outdated transitive dependencies.
    """
    modified_pyproject: dict[str, Any] = deps.get_modified_pyproject(
        PYPROJECT_TOML_PATH
    )
    main_dependencies = get_main_deps_pyproject(modified_pyproject)
    optional_dependencies = get_optional_deps_pyproject(modified_pyproject)
    dev_dependencies = get_deps_dev()

    outdated_main = get_outdated_deps(main_dependencies)
    outdated_optional = get_outdated_deps(optional_dependencies)
    outdated_dev = get_outdated_deps(dev_dependencies)

    found_outdated = any([outdated_main, outdated_optional, outdated_dev])
    transitive_headers = ("PACKAGE", "SPECIFIED", "AVAILABLE")
    if outdated_main:
        location = PYPROJECT_TOML_PATH.name + " - dependencies"
        cli.echo_failure(f"Outdated dependencies from {location}:")
        print_table(outdated_main, transitive_headers)
    if outdated_optional:
        location = PYPROJECT_TOML_PATH.name + " - optional-dependencies"
        cli.echo_failure(f"Outdated dependencies from {location}:")
        print_table(outdated_optional, transitive_headers)
    if outdated_dev:
        cli.echo_failure(f"Outdated dependencies from {DEV_DEPS_PATH.name}:")
        print_table(outdated_dev, transitive_headers)

    if not found_outdated:
        cli.echo_success("All top-level dependencies up to date.")

    if transitive:
        top_level: set[str] = {
            item.name for item in outdated_main + outdated_optional + outdated_dev
        }

        print("\nRetrieving transitive dependency information...")
        transitive_dependencies = lock_deps.get_lock_file_deps(
            LOCK_FILE_PATH, exclude=top_level
        )
        outdated_transitive = get_outdated_deps(transitive_dependencies, strip=True)

        if outdated_transitive:
            transitive_headers = ("PACKAGE", "PINNED", "AVAILABLE")

            cli.echo_failure("Outdated transitive dependencies:")
            print_table(outdated_transitive, transitive_headers)
        else:
            cli.echo_success("All transitive dependencies up to date.")


if __name__ == "__main__":
    cli.run(main)
