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

"""This script evaluates the entries in .static_files, .mandatory_files and
.deprecated_files and compares them with the microservice template repository,
or verifies their existence or non-existence depending on the list they are in.
"""

import difflib
import os
import shutil
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from script_utils.cli import echo_failure, echo_success, run
except ImportError:
    echo_failure = echo_success = print

    def run(main_fn):
        """Run main function without cli tools (typer)."""
        main_fn(check="--check" in sys.argv[1:])


REPO_ROOT_DIR = Path(__file__).parent.parent.absolute()

FILE_LIST_DIR_NAME = ".template"
DEPRECATED_FILES = "deprecated_files"
MANDATORY_FILES = "mandatory_files"
STATIC_FILES = "static_files"

IGNORE_SUFFIX = "_ignore"

TEMPLATE_LIST_REL_PATHS = [
    f"{FILE_LIST_DIR_NAME}/{list_name}.txt"
    for list_name in [STATIC_FILES, MANDATORY_FILES, DEPRECATED_FILES]
]

RAW_TEMPLATE_URL = (
    "https://raw.githubusercontent.com/ghga-de/microservice-repository-template/main/"
)


class ValidationError(RuntimeError):
    """Raised when files need to be updated."""


def get_file_list_path(list_name: str, relative: bool = False) -> Path:
    """Get the path to the file list of the given name."""
    return Path(REPO_ROOT_DIR / FILE_LIST_DIR_NAME / f"{list_name}.txt")


def get_file_list(list_name: str) -> list[str]:
    """Return a list of all file names specified in a given list file."""
    list_path = get_file_list_path(list_name)
    with open(list_path, encoding="utf8") as list_file:
        file_list = [
            clean_line
            for clean_line in (
                line.rstrip() for line in list_file if not line.startswith("#")
            )
            if clean_line
        ]
    if not list_name.endswith(IGNORE_SUFFIX):
        ignore_list_name = list_name + IGNORE_SUFFIX
        try:
            file_set_ignore = set(get_file_list(ignore_list_name))
        except FileNotFoundError:
            print(f"  - {ignore_list_name} is missing, no exceptions from the template")
        else:
            file_list = [line for line in file_list if line not in file_set_ignore]
    return file_list


def get_template_file_content(relative_file_path: str):
    """Get the content of the template file corresponding to the given path."""
    remote_file_url = urllib.parse.urljoin(RAW_TEMPLATE_URL, relative_file_path)
    remote_file_request = urllib.request.Request(remote_file_url)
    try:
        with urllib.request.urlopen(remote_file_request) as remote_file_response:
            return remote_file_response.read().decode(
                remote_file_response.headers.get_content_charset("utf-8")
            )
    except urllib.error.HTTPError as remote_file_error:
        print(
            f"  - WARNING: request to remote file {remote_file_url} returned"
            f" status code {remote_file_error.code}"
        )
        return None


def diff_content(local_file_path, local_file_content, template_file_content) -> bool:
    """Show diff between given local and remote template file content."""
    if local_file_content != template_file_content:
        print(f"  - {local_file_path}: differs from template")
        for line in difflib.unified_diff(
            template_file_content.splitlines(keepends=True),
            local_file_content.splitlines(keepends=True),
            fromfile="template",
            tofile="local",
        ):
            print("   ", line.rstrip())
        return True
    return False


def check_file(relative_file_path: str, diff: bool = False) -> bool:
    """Compare file at the given path with the given content.

    Returns True if there are differences.
    """
    local_file_path = REPO_ROOT_DIR / Path(relative_file_path)

    if not local_file_path.exists():
        print(f"  - {local_file_path} does not exist")
        return True

    if diff:
        template_file_content = get_template_file_content(relative_file_path)

        if template_file_content is None:
            print(f"  - {local_file_path}: cannot check, remote is missing")
            return True

        with open(local_file_path, encoding="utf8") as file:
            return diff_content(local_file_path, file.read(), template_file_content)

    return False


def update_file(relative_file_path: str, diff: bool = False) -> bool:
    """Update file at the given relative path.

    Returns True if there are updates.
    """

    local_file_path = REPO_ROOT_DIR / Path(relative_file_path)
    local_parent_dir = local_file_path.parent

    if not local_parent_dir.exists():
        local_parent_dir.mkdir(parents=True)

    if diff or not local_file_path.exists():
        template_file_content = get_template_file_content(relative_file_path)

        if template_file_content is None:
            print(f"  - {local_file_path}: cannot update, remote is missing")
            return True

        if diff and local_file_path.exists():
            with open(local_file_path, encoding="utf8") as file:
                if file.read() == template_file_content:
                    return False

        executable = template_file_content.startswith("#!")
        executable_flags = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        with open(local_file_path, "w", encoding="utf8") as file:
            file.write(template_file_content)
            mode = os.fstat(file.fileno()).st_mode
            if executable:
                mode |= executable_flags
            else:
                mode &= ~executable_flags
            os.fchmod(file.fileno(), stat.S_IMODE(mode))

        print(f"  - {local_file_path}: updated")
        return True

    return False


def update_files(files: list[str], check: bool = False, diff: bool = False) -> bool:
    """Update or check all the files in the given list.

    Returns True if there are updates.
    """
    updates = False
    update_or_check_file = check_file if check else update_file
    for relative_file_path in files:
        if update_or_check_file(relative_file_path, diff=diff):
            updates = True
    return updates


def remove_files(files: list[str], check: bool = False) -> bool:
    """Remove or check all the files in the given list.

    Returns True if there are updates.
    """
    updates = False
    for relative_file_path in files:
        local_file_path = REPO_ROOT_DIR / Path(relative_file_path)

        if local_file_path.exists():
            if check:
                print(f"  - {local_file_path}: deprecated, but exists")
            else:
                if local_file_path.is_dir():
                    shutil.rmtree(local_file_path)
                else:
                    local_file_path.unlink()
                print(f"  - {local_file_path}: removed, since it is deprecated")
            updates = True
    return updates


def main(check: bool = False):
    """Update the static files in the service template."""
    updated = False

    print("Template lists...")
    if update_files(TEMPLATE_LIST_REL_PATHS, diff=True, check=False):
        updated = True

    print("Static files...")
    files_to_update = get_file_list(STATIC_FILES)
    if update_files(files_to_update, diff=True, check=check):
        updated = True

    print("Mandatory files...")
    files_to_guarantee = get_file_list(MANDATORY_FILES)
    if update_files(files_to_guarantee, check=check):
        updated = True

    print("Deprecated files...")
    files_to_remove = get_file_list(DEPRECATED_FILES)
    if remove_files(files_to_remove, check=check):
        updated = True

    if check:
        if updated:
            echo_failure("Validating files from template failed.")
            sys.exit(1)
        echo_success("Successfully validated files from template.")
    else:
        echo_success(
            "Successfully updated files from template."
            if updated
            else "No updates from the template were necessary."
        )


if __name__ == "__main__":
    run(main)
