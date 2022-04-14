#!/usr/bin/env python3

# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

"""This script checks the content of all files considered static (as defined in `../.static_files`)
by comparing it to the corresponding counterparts in the template repository.
This script reqiures git to be available on PATH.
"""

import subprocess
import sys
import tempfile
from collections import namedtuple
from pathlib import Path
from typing import List, Literal

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
STATIC_FILE_LIST = REPO_ROOT_DIR / ".static_files"
MANDATORY_FILE_LIST = REPO_ROOT_DIR / ".mandatory_files"
TEMPLATE_REPO_URL = "https://github.com/ghga-de/microservice-repository-template.git"

FileCheck = namedtuple("FileCheck", ["rel_path", "result"])


def clone_the_template_repo(target_dir: Path):
    """Clone the template repo to the specified target dir."""
    with subprocess.Popen(
        args=["git", "clone", TEMPLATE_REPO_URL, target_dir.absolute()],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) as process:
        exit_code = process.wait()
        assert (
            exit_code == 0
        ), f"Cloning template repository failed with exit code {exit_code}"


class TemplateRepo:
    """Context manager for temporary cloning the template repo."""

    def __init__(self):
        """Initialize temporary directory."""
        # pylint: disable=consider-using-with
        self.temp_dir_context_manager = tempfile.TemporaryDirectory()
        self.temp_dir = None

    def __enter__(self) -> Path:
        """create temp_dir and clone the template repo into it"""
        self.temp_dir = Path(self.temp_dir_context_manager.__enter__()).absolute()
        clone_the_template_repo(self.temp_dir)
        return self.temp_dir

    def __exit__(self, exc_type, exc, traceback):
        """cleanup template repo"""
        self.temp_dir_context_manager.__exit__(exc_type, exc, traceback)


def get_files_list(list_file_path: Path) -> List[Path]:
    """returns a list of static files paths"""

    static_files: List[Path] = []

    with open(list_file_path, "r", encoding="utf8") as list_file:
        for line in list_file:
            relative_file_path = line.rstrip("\n")

            if relative_file_path == "" or relative_file_path.startswith("#"):
                continue

            static_files.append(Path(relative_file_path))

    return static_files


def compare_content_of_files(file_a: Path, file_b: Path) -> bool:
    """Compares the content of two files.
    Returns True if matching and False if missmatching.
    """

    with subprocess.Popen(
        args=["diff", file_a.absolute(), file_b.absolute()],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ) as process:

        # check if files differ based on exit code:
        do_match = process.wait() == 0

        # print diff:
        if not do_match:
            diff = "" if process.stdout is None else str(process.stdout.read())
            print(f"\nERROR: difference detected:\n{diff}\n")

    return do_match


def check_file(
    relative_file_path: Path, template_root_dir: Path, check_content: bool = True
) -> Literal["MISSING", "MISSMATCH", "TEMPLATE_MISSING", "OK"]:
    """Checks existence and content of a static file"""

    local_file_path = REPO_ROOT_DIR / relative_file_path

    if not local_file_path.exists():
        print("\nERROR: File not found.\n")
        return "MISSING"

    if check_content:
        template_file_path = template_root_dir / relative_file_path

        if not template_file_path.exists():
            print("\nERROR: The template file is missing.\n")
            return "TEMPLATE_MISSING"

        if not compare_content_of_files(local_file_path, template_file_path):
            return "MISSMATCH"

    return "OK"


def check_all_mandatory_files():
    """Checks existence of all mandatory files"""
    mandatory_files = get_files_list(MANDATORY_FILE_LIST)
    failed_checks: List[FileCheck] = []

    with TemplateRepo() as template_root_dir:

        print("\nChecking existence of all mandatory files:")

        for mandatory_file in mandatory_files:
            print(f"  - {mandatory_file}")
            result = check_file(mandatory_file, template_root_dir, check_content=False)

            if result != "OK":
                failed_checks.append(FileCheck(mandatory_file, result))

    return failed_checks


def check_all_static_files():
    """Checks existence and content of all static files"""
    static_files = get_files_list(STATIC_FILE_LIST)
    failed_checks: List[FileCheck] = []

    with TemplateRepo() as template_root_dir:

        print("\nChecking content and existence of all static files:")

        for static_file in static_files:
            print(f"  - {static_file}")
            result = check_file(static_file, template_root_dir, check_content=True)

            if result != "OK":
                failed_checks.append(FileCheck(static_file, result))

    return failed_checks


def print_summary(failed_checks: List[FileCheck], category: str):
    """Print summary of file checks."""

    if len(failed_checks) == 0:
        print(f"\nAll {category} files passed the check.")
    else:
        print(f"\nFollowing {category} files failed the checks:")
        for failed_check in failed_checks:
            print(f"  - {failed_check.rel_path}: {failed_check.result}")


def run():
    """Check all static and mandatory files."""

    mandatory_static_checks = check_all_mandatory_files()
    failed_static_checks = check_all_static_files()

    print_summary(mandatory_static_checks, "MANDATORY")
    print_summary(failed_static_checks, "STATIC")

    if len(failed_static_checks) != 0 or len(mandatory_static_checks) != 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
