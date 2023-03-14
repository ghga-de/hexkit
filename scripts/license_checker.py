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

# pylint: skip-file

"""This script checks that the license and license headers
exists and that they are up to date.
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple, Union

# root directory of the package:
ROOT_DIR = Path(__file__).parent.parent.resolve()

# file containing the default global copyright notice:
GLOBAL_COPYRIGHT_FILE_PATH = ROOT_DIR / ".devcontainer" / "license_header.txt"

# exclude files and dirs from license header check:
EXCLUDE = [
    ".devcontainer",
    "eggs",
    ".eggs",
    "dist",
    "build",
    "develop-eggs",
    "lib",
    "lib62",
    "parts",
    "sdist",
    "wheels",
    "pip-wheel-metadata",
    ".git",
    ".github",
    ".flake8",
    ".gitignore",
    ".pylintrc",
    "example_config.yaml",
    "config_schema.json",
    "LICENSE",  # is checked but not for the license header
    ".pre-commit-config.yaml",
    "docs",
    ".vscode",
    ".mypy_cache",
    ".mypy.ini",
    ".pytest_cache",
    ".editorconfig",
    ".static_files",
    ".static_files_ignore",
    ".mandatory_files",
    ".mandatory_files_ignore",
    ".deprecated_files",
    ".deprecated_files_ignore",
]

# exclude file by file ending from license header check:
EXCLUDE_ENDINGS = [
    "html",
    "ini",
    "jinja",
    "json",
    "md",
    "pub",
    "pyc",
    "sec",
    "txt",
    "xml",
    "yaml",
    "yml",
]

# exclude any files with names that match any of the following regex:
EXCLUDE_PATTERN = [r".*\.egg-info.*", r".*__cache__.*", r".*\.git.*"]

# The License header, "{year}" will be replaced by current year:
COPYRIGHT_TEMPLATE = """Copyright {year} {author}

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

# A list of all chars that may be used to introduce a comment:
COMMENT_CHARS = ["#"]

AUTHOR = """Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
for the German Human Genome-Phenome Archive (GHGA)"""

# The copyright notice should not date earlier than this year:
MIN_YEAR = 2021

# The path to the License file relative to target dir
LICENSE_FILE = "LICENSE"


class GlobalCopyrightNotice:
    """
    This is used to store the copyright notice that should be identical for all checked
    files.
    The text of the copyright notice is stored in the `text`
    property. This property can only be set once.
    The property `n_lines` gives the number of lines of the text. It is inferred once
    `text` is set.
    """

    def __init__(self):
        self._text: Optional[str] = None
        self._n_lines: Optional[int] = None

    @property
    def text(self) -> Optional[str]:
        return self._text

    @text.setter
    def text(self, new_text: str):
        if self._text is not None:
            raise RuntimeError("You can only set the value once.")
        self._text = new_text
        self._n_lines = len(self._text.split("\n"))

    @property
    def n_lines(self) -> int:
        if self._n_lines is None:
            raise ValueError(
                "This property is not yet available."
                + " Please set the `text` property first."
            )
        return self._n_lines


class UnexpectedBinaryFileError(RuntimeError):
    """Thrown when trying to read a binary file."""

    def __init__(self, file_path: Union[str, Path]):
        message = f"The file could not be read because it is binary: {str(file_path)}"
        super().__init__(message)


def get_target_files(
    target_dir: Path,
    exclude: List[str] = EXCLUDE,
    exclude_endings: List[str] = EXCLUDE_ENDINGS,
    exclude_pattern: List[str] = EXCLUDE_PATTERN,
) -> List[Path]:
    """Get target files that are not match the exclude conditions.
    Args:
        target_dir (pathlib.Path): The target dir to search.
        exclude (List[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (List[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (List[str], optional):
            Overwrite default list of regex patterns match file path
            for exclusion.
    """
    abs_target_dir = Path(target_dir).absolute()
    exclude_normalized = [(abs_target_dir / excl).absolute() for excl in exclude]

    # get all files:
    all_files = [
        file_.absolute() for file_ in Path(abs_target_dir).rglob("*") if file_.is_file()
    ]

    target_files = [
        file_
        for file_ in all_files
        if not (
            any([file_.is_relative_to(excl) for excl in exclude_normalized])
            or any([str(file_).endswith(ending) for ending in exclude_endings])
            or any([re.match(pattern, str(file_)) for pattern in exclude_pattern])
        )
    ]
    return target_files


def normalized_line(line: str, chars_to_trim: List[str] = COMMENT_CHARS) -> str:
    norm_line = line.strip()

    for char in chars_to_trim:
        norm_line = norm_line.strip(char)

    return norm_line.strip("\n").strip("\t").strip()


def normalized_text(text: str, chars_to_trim: List[str] = COMMENT_CHARS) -> str:
    "Normalize a license header text."
    lines = text.split("\n")

    norm_lines: List[str] = []

    for line in lines:
        stripped_line = line.strip()
        # exclude shebang:
        if stripped_line.startswith("#!"):
            continue

        norm_line = normalized_line(stripped_line)

        # exclude empty lines:
        if norm_line == "":
            continue

        norm_lines.append(norm_line)

    return "\n".join(norm_lines).strip("\n")


def format_copyright_template(copyright_template: str, author: str) -> str:
    """Formats license header by inserting the specified author for every occurence of
    "{author}" in the header template.
    """
    return normalized_text(copyright_template.replace("{author}", author))


def is_commented_line(line: str, comment_chars: List[str] = COMMENT_CHARS) -> bool:
    """Checks whether a line is a comment."""
    line_stripped = line.strip()
    for comment_char in comment_chars:
        if line_stripped.startswith(comment_char):
            return True

    return False


def is_empty_line(line: str) -> bool:
    """Checks whether a line is empty."""
    return line.strip("\n").strip("\t").strip() == ""


def get_header(file_path: Path, comment_chars: List[str] = COMMENT_CHARS):
    """Extracts the header from a file and normalizes it."""
    header_lines: List[str] = []

    try:
        with open(file_path, "r") as file:
            for line in file:
                if is_commented_line(
                    line, comment_chars=comment_chars
                ) or is_empty_line(line):
                    header_lines.append(line)
                else:
                    break
    except UnicodeDecodeError as error:
        raise UnexpectedBinaryFileError(file_path=file_path) from error

    # normalize the lines:
    header = "".join(header_lines)
    return normalized_text(header, chars_to_trim=comment_chars)


def validate_year_string(year_string: str, min_year: int = MIN_YEAR) -> bool:
    """Check if the specified year string is valid.
    Returns `True` if valid or `False` otherwise."""

    current_year = date.today().year

    # If the year_string is a single number, it must be the current year:
    if year_string.isnumeric():
        return int(year_string) == current_year

    # Otherwise, a range (e.g. 2021 - 2023) is expected:
    match = re.match("(\d+) - (\d+)", year_string)

    if not match:
        return False

    year_1 = int(match.group(1))
    year_2 = int(match.group(2))

    # Check the validity of the range:
    if year_1 >= min_year and year_2 <= year_1:
        return False

    # year_2 must be equal to the current year:
    return year_2 == current_year


def check_copyright_notice(
    copyright: str,
    global_copyright: GlobalCopyrightNotice,
    copyright_template: str = COPYRIGHT_TEMPLATE,
    author: str = AUTHOR,
    comment_chars: List[str] = COMMENT_CHARS,
    min_year: int = MIN_YEAR,
) -> bool:
    """Checks the specified copyright text against a template.

    copyright_template (str):
        A string containing the copyright text to check against the template.
    global_copyright (str, None):
        If this is a string, it is checked whether the copyright notice in this file
        contains the same year string.
        If this is None, the variable is set to the year string present in the
        copyright notice of this file.
    copyright_template (str, optional):
        A string containing a template for the expected license header.
        You may include "{year}" which will be replace by the current year.
        This defaults to the Apache 2.0 Copyright notice.
    author (str, optional):
        The author that shall be included in the license header.
        It will replace any appearance of "{author}" in the license
        header. This defaults to an auther info for GHGA.

    """
    # If the global_copyright is already set, check if the current copyright is
    # identical to it:
    copyright_lines = copyright.split("\n")
    if global_copyright.text is not None:
        copyright_cleaned = "\n".join(copyright_lines[0 : global_copyright.n_lines])
        return global_copyright.text == copyright_cleaned

    formatted_template = format_copyright_template(copyright_template, author=author)
    template_lines = formatted_template.split("\n")

    # The header should be at least as long as the template:
    if len(copyright_lines) < len(template_lines):
        return False

    for idx, template_line in enumerate(template_lines):
        header_line = copyright_lines[idx]

        if "{year}" in template_line:
            pattern = template_line.replace("{year}", r"(.+?)")
            match = re.match(pattern, header_line)

            if not match:
                return False

            year_string = match.group(1)
            if not validate_year_string(year_string, min_year=min_year):
                return False

        elif template_line != header_line:
            return False

    # Take this copyright as the global_copyright from now on:
    copyright_cleaned = "\n".join(copyright_lines[0 : len(template_line)])
    global_copyright.text = copyright_cleaned

    return True


def check_file_headers(
    target_dir: Path,
    global_copyright: GlobalCopyrightNotice,
    copyright_template: str = COPYRIGHT_TEMPLATE,
    author: str = AUTHOR,
    exclude: List[str] = EXCLUDE,
    exclude_endings: List[str] = EXCLUDE_ENDINGS,
    exclude_pattern: List[str] = EXCLUDE_PATTERN,
    comment_chars: List[str] = COMMENT_CHARS,
    min_year: int = MIN_YEAR,
) -> Tuple[List[Path], List[Path]]:
    """Check files for presence of a license header and verify that
    the copyright notice is up to date (correct year).

    Args:
        target_dir (pathlib.Path): The target dir to search.
        copyright_template (str, optional):
            A string containing a template for the expected license header.
            You may include "{year}" which will be replace by the current year.
            This defaults to the Apache 2.0 Copyright notice.
        global_copyright (str, None):
            If this is a string, it is checked whether the copyright notice of these
            files contains the same year string.
            If this is None, the variable is set to the year string present in the
            copyright notice of these files.
        author (str, optional):
            The author that shall be included in the license header.
            It will replace any appearance of "{author}" in the license
            header. This defaults to an author info for GHGA.
        exclude (List[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (List[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (List[str], optional):
            Overwrite default list of regex patterns match file path
            for exclusion.
    """
    target_files = get_target_files(
        target_dir,
        exclude=exclude,
        exclude_endings=exclude_endings,
        exclude_pattern=exclude_pattern,
    )

    # check if license header present in file:
    passed_files: List[Path] = []
    failed_files: List[Path] = []

    for target_file in target_files:
        try:
            header = get_header(target_file, comment_chars=comment_chars)
            if check_copyright_notice(
                copyright=header,
                global_copyright=global_copyright,
                copyright_template=copyright_template,
                author=author,
                comment_chars=comment_chars,
                min_year=min_year,
            ):
                passed_files.append(target_file)
            else:
                failed_files.append(target_file)
        except UnexpectedBinaryFileError:
            # This file is a binary and is therefor skipped.
            pass

    return (passed_files, failed_files)


def check_license_file(
    license_file: Path,
    global_copyright: GlobalCopyrightNotice,
    copyright_template: str = COPYRIGHT_TEMPLATE,
    author: str = AUTHOR,
    comment_chars: List[str] = COMMENT_CHARS,
    min_year: int = MIN_YEAR,
) -> bool:
    """Currently only checks if the copyright notice in the
    License file is up to data.

    Args:
        license_file (pathlib.Path, optional): Overwrite the default license file.
        global_copyright (str, None):
            If this is a string, it is checked whether the copyright notice in this file
            contains the same year string.
            If this is None, the variable is set to the year string present in the
            copyright notice of this file.
        copyright_template (str, optional):
            A string of the copyright notice (usually same as license header).
            You may include "{year}" which will be replace by the current year.
            This defaults to the Apache 2.0 Copyright notice.
        author (str, optional):
            The author that shall be included in the copyright notice.
            It will replace any appearance of "{author}" in the copyright
            notice. This defaults to an author info for GHGA.
    """

    if not license_file.is_file():
        print(f'Could not find license file "{str(license_file)}".')
        return False

    with open(license_file, "r") as file_:
        license_text = normalized_text(file_.read())

    # Extract the copyright notice:
    # (is expected to be at the end of the file):
    formatted_template = format_copyright_template(copyright_template, author=author)
    template_lines = formatted_template.split("\n")
    license_lines = license_text.split("\n")
    copyright = "\n".join(license_lines[-len(template_lines) :])

    return check_copyright_notice(
        copyright=copyright,
        global_copyright=global_copyright,
        copyright_template=copyright_template,
        author=author,
        comment_chars=comment_chars,
        min_year=min_year,
    )


def run():
    """Run checks from CLI."""
    parser = argparse.ArgumentParser(
        prog="license-checker",
        description=(
            "This script checks that the license and license headers "
            + "exists and that they are up to date."
        ),
    )

    parser.add_argument(
        "-L",
        "--no-license-file-check",
        help="Disables the check of the license file",
        action="store_true",
    )

    parser.add_argument(
        "-t",
        "--target-dir",
        help="Specify a custom target dir. Overwrites the default package root.",
    )

    args = parser.parse_args()

    target_dir = Path(args.target_dir).absolute() if args.target_dir else ROOT_DIR

    print(f'Working in "{target_dir}"\n')

    global_copyright = GlobalCopyrightNotice()

    # get global copyright from .devcontainer/license_header.txt file:
    with open(GLOBAL_COPYRIGHT_FILE_PATH, "r") as global_copyright_file:
        global_copyright.text = normalized_text(global_copyright_file.read())

    if args.no_license_file_check:
        license_file_valid = True
    else:
        license_file = Path(target_dir / LICENSE_FILE)
        print(f'Checking if LICENSE file is up to date: "{license_file}"')
        license_file_valid = check_license_file(
            license_file, global_copyright=global_copyright
        )
        print(
            "Copyright notice in license file is "
            + ("" if license_file_valid else "not ")
            + "up to date.\n"
        )

    print("Checking license headers in files:")
    passed_files, failed_files = check_file_headers(
        target_dir, global_copyright=global_copyright
    )
    print(f"{len(passed_files)} files passed.")
    print(f"{len(failed_files)} files failed" + (":" if failed_files else "."))
    for failed_file in failed_files:
        print(f'  - "{failed_file.relative_to(target_dir)}"')

    print("")

    if failed_files or not license_file_valid:
        print("Some checks failed.")
        sys.exit(1)

    print("All checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    run()
