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

"""Updates OpenAPI-based documentation"""

import sys
from difflib import unified_diff
from pathlib import Path

import yaml
from script_utils.cli import echo_failure, echo_success, run
from script_utils.fastapi_app_location import app

HERE = Path(__file__).parent.resolve()
REPO_ROOT_DIR = HERE.parent
OPENAPI_YAML = REPO_ROOT_DIR / "openapi.yaml"


class ValidationError(RuntimeError):
    """Raised when validation of OpenAPI documentation fails."""


def get_openapi_spec() -> str:
    """Get an OpenAPI spec in YAML format from the main FastAPI app as defined in the
    fastapi_app_location.py file.
    """

    openapi_spec = app.openapi()
    return yaml.safe_dump(openapi_spec)


def update_docs():
    """Update the OpenAPI YAML file located in the repository's root dir."""

    openapi_spec = get_openapi_spec()
    with open(OPENAPI_YAML, "w", encoding="utf-8") as openapi_file:
        openapi_file.write(openapi_spec)


def print_diff(expected: str, observed: str):
    """Print differences between expected and observed files."""
    echo_failure("Differences in OpenAPI YAML:")
    for line in unified_diff(
        expected.splitlines(keepends=True),
        observed.splitlines(keepends=True),
        fromfile="expected",
        tofile="observed",
    ):
        print("   ", line.rstrip())


def check_docs():
    """Checks whether the OpenAPI YAML file located in the repository's root dir is up
    to date.

    Raises:
        ValidationError: if not up to date.
    """

    openapi_expected = get_openapi_spec()
    with open(OPENAPI_YAML, "r", encoding="utf-8") as openapi_file:
        openapi_observed = openapi_file.read()

    if openapi_expected != openapi_observed:
        print_diff(openapi_expected, openapi_observed)
        raise ValidationError(
            f"The OpenAPI YAML at '{OPENAPI_YAML}' is not up to date."
        )


def main(check: bool = False):
    """Update or check the OpenAPI documentation."""

    if check:
        try:
            check_docs()
        except ValidationError as error:
            echo_failure(f"Validation failed: {error}")
            sys.exit(1)
        echo_success("OpenAPI docs are up to date.")
        return

    update_docs()
    echo_success("Successfully updated the OpenAPI docs.")


if __name__ == "__main__":
    run(main)
