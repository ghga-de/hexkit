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

"""Update the dependency lock files located at 'requirements.txt' and
'requirements-dev.txt'.
"""

import os
import re
import subprocess
from itertools import zip_longest
from pathlib import Path
from tempfile import TemporaryDirectory

import tomli_w

from script_utils import cli, deps

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
LOCK_DIR = REPO_ROOT_DIR / "lock"

PYPROJECT_TOML_PATH = REPO_ROOT_DIR / "pyproject.toml"
DEV_DEPS_PATH = LOCK_DIR / "requirements-dev.in"
OUTPUT_LOCK_PATH = LOCK_DIR / "requirements.txt"
OUTPUT_DEV_LOCK_PATH = LOCK_DIR / "requirements-dev.txt"


def fix_temp_dir_comments(file_path: Path):
    """Fix the temp_dir comments so they don't cause a noisy diff

    This will leave the top compile message intact as a point of sanity to verify that
    the requirements are indeed being generated if nothing else changes.
    """

    with open(file_path, encoding="utf-8") as file:
        lines = file.readlines()

    with open(file_path, "w", encoding="utf-8") as file:
        for line in lines:
            # Remove random temp directory name
            line = re.sub(
                r"\([^\)\(]*?pyproject\.toml\)",
                "(pyproject.toml)",
                line,
            )
            file.write(line)


def is_file_outdated(old_file: Path, new_file: Path) -> bool:
    """Compares two lock files and returns True if there is a difference, else False"""

    outdated = False

    with open(old_file, encoding="utf-8") as old:
        with open(new_file, encoding="utf-8") as new:
            outdated = any(
                old_line != new_line
                for old_line, new_line in zip_longest(
                    (
                        line
                        for line in (line.strip() for line in old)
                        if line and not line.startswith("#")
                    ),
                    (
                        line
                        for line in (line.strip() for line in new)
                        if line and not line.startswith("#")
                    ),
                )
            )
    if outdated:
        cli.echo_failure(f"{str(old_file)} is out of date!")
    return outdated


def compile_lock_file(
    sources: list[Path],
    output: Path,
    upgrade: bool,
    extras: bool,
) -> None:
    """From the specified sources compile a lock file using pip-compile from pip-tools
    and write it to the specified output location.
    """

    print(f"Updating '{output.name}'...")

    command = ["uv", "pip", "compile", "--refresh", "--generate-hashes", "--no-header"]

    if upgrade:
        command.append("--upgrade")

    if extras:
        command.append("--all-extras")

    command.extend(["--output-file", str(output.absolute())])

    command.extend([str(source.absolute()) for source in sources])

    # constrain the production deps by what's pinned in requirements-dev.txt
    if output.name == OUTPUT_LOCK_PATH.name:
        command.extend(["-c", str(OUTPUT_DEV_LOCK_PATH)])

    completed_process = subprocess.run(
        args=command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed_process.returncode != 0:
        std_out = completed_process.stdout
        log = std_out.decode("utf-8") if std_out else "no log available."
        raise RuntimeError(f"Failed to compile lock file:\n{log}")

    fix_temp_dir_comments(output.absolute())


def ensure_lock_files_exist():
    """Make sure that the lock files exist if in check mode"""
    for output in [OUTPUT_DEV_LOCK_PATH, OUTPUT_LOCK_PATH]:
        if not os.path.exists(output):
            cli.echo_failure(f"{output} is missing")
            return


def main(upgrade: bool = False, check: bool = False):
    """Update the dependency lock files located at 'requirements.txt' and
    'requirements-dev.txt'.

    For the 'requirements.txt' only the package with its dependencies being defined in
    the 'pyproject.toml' are considered. Thereby, all recursive dependencies of the
    package on itself are removed.

    For the 'requirements-dev.txt', in addition to the filtered 'pyproject.toml' the
    'requirements-dev.in' is considered.

    The `upgrade` parameter can be used to indicate that dependencies found in existing
    lock files should be upgraded. Default pip-compile behavior is to leave them as is.
    """

    # if --check is used, quickly ensure that there is something to compare against
    if check:
        ensure_lock_files_exist()

    modified_pyproject = deps.get_modified_pyproject(PYPROJECT_TOML_PATH)

    extras = (
        "optional-dependencies" in modified_pyproject["project"]
        and modified_pyproject["project"]["optional-dependencies"]
    )
    with TemporaryDirectory() as temp_dir:
        modified_pyproject_path = Path(temp_dir) / "pyproject.toml"
        with open(modified_pyproject_path, "wb") as modified_pyproject_toml:
            tomli_w.dump(modified_pyproject, modified_pyproject_toml)

        # make src dir next to TOML to satisfy build system
        os.makedirs(Path(temp_dir) / "src")

        # temporary test files
        check_dev_path = Path(temp_dir) / OUTPUT_DEV_LOCK_PATH.name
        check_prod_path = Path(temp_dir) / OUTPUT_LOCK_PATH.name

        # compile requirements-dev.txt (includes all dependencies)
        compile_lock_file(
            sources=[modified_pyproject_path, DEV_DEPS_PATH],
            output=check_dev_path if check else OUTPUT_DEV_LOCK_PATH,
            upgrade=upgrade,
            extras=extras,
        )

        if check and is_file_outdated(OUTPUT_DEV_LOCK_PATH, check_dev_path):
            return

        # compile requirements.txt (only includes production-related subset of above)
        compile_lock_file(
            sources=[modified_pyproject_path],
            output=check_prod_path if check else OUTPUT_LOCK_PATH,
            upgrade=upgrade,
            extras=extras,
        )

        if check and is_file_outdated(OUTPUT_LOCK_PATH, check_prod_path):
            return

    if check:
        cli.echo_success("Lock files are up to date.")
    else:
        cli.echo_success(
            f"Successfully updated lock files at '{OUTPUT_LOCK_PATH}' and"
            + f" '{OUTPUT_DEV_LOCK_PATH}'."
        )


if __name__ == "__main__":
    cli.run(main)
