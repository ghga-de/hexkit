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
#

"""A script to update the pyproject.toml."""

import sys
import tomllib
from pathlib import Path

import tomli_w

from script_utils import cli

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
PYPROJECT_GENERATION_DIR = REPO_ROOT_DIR / ".pyproject_generation"

PYPROJECT_TEMPLATE_PATH = PYPROJECT_GENERATION_DIR / "pyproject_template.toml"
PYPROJECT_CUSTOM_PATH = PYPROJECT_GENERATION_DIR / "pyproject_custom.toml"
PYPROJECT_TOML = REPO_ROOT_DIR / "pyproject.toml"


def read_template_pyproject() -> dict[str, object]:
    """Read the pyproject_template.toml."""
    with open(PYPROJECT_TEMPLATE_PATH, "rb") as file:
        return tomllib.load(file)


def read_custom_pyproject() -> dict[str, object]:
    """Read the pyproject_custom.toml."""
    with open(PYPROJECT_CUSTOM_PATH, "rb") as file:
        return tomllib.load(file)


def read_current_pyproject() -> dict[str, object]:
    """Read the current pyproject.toml."""
    with open(PYPROJECT_TOML, "rb") as file:
        return tomllib.load(file)


def write_pyproject(pyproject: dict[str, object]) -> None:
    """Write the given pyproject dict into the pyproject.toml."""
    with open(PYPROJECT_TOML, "wb") as file:
        tomli_w.dump(pyproject, file, multiline_strings=True)


def merge_fields(*, source: dict[str, object], dest: dict[str, object]):
    """Merge fields existing in both custom and template pyproject definitions.

    If a given field is a dictionary, merge or assign depending on if it's found in dest.
    If the field is anything else either assign the value or exit with an error message
    if the values have different types.
    """
    for field, value in source.items():
        if isinstance(value, dict):
            if field in dest:
                merge_fields(source=source[field], dest=dest[field])  # type: ignore
            else:
                dest[field] = value
        else:
            if field in dest:
                if type(value) == type(dest[field]):
                    cli.echo_warning(f"Overriding value for '{field}'...")
                else:
                    cli.echo_failure(f"Conflicting types for '{field}'...")
                    sys.exit(1)
            dest[field] = value


def merge_pyprojects(inputs: list[dict[str, object]]) -> dict[str, object]:
    """Compile a pyproject dict from the provided input dicts."""
    pyproject = inputs[0]

    for input in inputs[1:]:
        for field, value in input.items():
            if field in pyproject:
                merge_fields(source=value, dest=pyproject[field])  # type: ignore
            else:
                pyproject[field] = value

    return pyproject


def main(*, check: bool = False):
    """Update the pyproject.toml or checks for updates if the check flag is specified."""
    template_pyproject = read_template_pyproject()
    custom_pyproject = read_custom_pyproject()
    merged_pyproject = merge_pyprojects([template_pyproject, custom_pyproject])

    if check:
        current_pyproject = read_current_pyproject()

        if current_pyproject != merged_pyproject:
            cli.echo_failure("The pyproject.toml is not up to date.")
            sys.exit(1)

        cli.echo_success("The pyproject.toml is up to date.")
        return

    write_pyproject(merged_pyproject)
    cli.echo_success("Successfully updated the pyproject.toml.")


if __name__ == "__main__":
    cli.run(main)
