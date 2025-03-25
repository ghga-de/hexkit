#!/usr/bin/env python3

# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""This script checks that the license and license headers
exist and that they are up to date.
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path

# root directory of the package:
ROOT_DIR = Path(__file__).parent.parent.resolve()

# file containing the default global copyright notice:
GLOBAL_COPYRIGHT_FILE_PATH = ROOT_DIR / ".devcontainer" / "license_header.txt"

# exclude files and dirs from license header check:
EXCLUDE = [
    ".coveragerc",
    ".devcontainer",
    ".editorconfig",
    ".eggs",
    ".git",
    ".github",
    ".flake8",
    ".gitignore",
    ".mypy_cache",
    ".mypy.ini",
    ".pylintrc",
    ".pytest_cache",
    ".ruff.toml",
    ".ruff_cache",
    ".template/.static_files.txt",
    ".template/.static_files_ignore.txt",
    ".template/.mandatory_files.txt",
    ".template/.mandatory_files_ignore.txt",
    ".template/.deprecated_files.txt",
    ".template/.deprecated_files_ignore.txt",
    ".tox",
    ".venv",
    ".vscode",
    "eggs",
    "build",
    "config_schema.json",
    "dist",
    "docs",
    "develop-eggs",
    "example_config.yaml",
    "htmlcov",
    "lib",
    "lib62",
    "parts",
    "pip-wheel-metadata",
    "sdist",
    "venv",
    "wheels",
    "LICENSE",  # is checked but not for the license header
]

# exclude file by file ending from license header check:
EXCLUDE_ENDINGS = [
    "html",
    "in",
    "ini",
    "jinja",
    "json",
    "md",
    "pub",
    "pyc",
    "pyd",
    "typed",
    "sec",
    "toml",
    "txt",
    "xml",
    "yaml",
    "yml",
    "tsv",
    "fastq",
    "gz",
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

# A list of strings that may be used to introduce a line comment:
LINE_COMMENTS = ["#"]

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
        self._text: str | None = None
        self._n_lines: int | None = None

    @property
    def text(self) -> str | None:
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

    def __init__(self, file_path: str | Path):
        message = f"The file could not be read because it is binary: {str(file_path)}"
        super().__init__(message)


def get_target_files(
    target_dir: Path,
    exclude: list[str] = EXCLUDE,
    exclude_endings: list[str] = EXCLUDE_ENDINGS,
    exclude_pattern: list[str] = EXCLUDE_PATTERN,
) -> list[Path]:
    """Get target files that are not match the exclude conditions.
    Args:
        target_dir (pathlib.Path): The target dir to search.
        exclude (list[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (list[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (list[str], optional):
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
            any(file_.is_relative_to(excl) for excl in exclude_normalized)
            or any(str(file_).endswith(ending) for ending in exclude_endings)
            or any(re.match(pattern, str(file_)) for pattern in exclude_pattern)
        )
    ]
    return target_files


def normalized_line(line: str, line_comments: list[str] = LINE_COMMENTS) -> str:
    line = line.strip()
    for line_comment in line_comments:
        line_without_comment = line.removeprefix(line_comment)
        if line_without_comment != line:
            line = line_without_comment.lstrip()
            break
    return line


def normalized_text(text: str, line_comments: list[str] = LINE_COMMENTS) -> str:
    "Normalize a license header text."
    lines = text.split("\n")

    norm_lines: list[str] = []

    for line in lines:
        stripped_line = line.strip()
        # exclude shebang:
        if stripped_line.startswith("#!"):
            continue

        norm_line = normalized_line(stripped_line, line_comments=line_comments)

        # exclude empty lines:
        if norm_line == "":
            continue

        norm_lines.append(norm_line)

    return "\n".join(norm_lines).strip("\n")


def format_copyright_template(copyright_template: str, author: str) -> str:
    """Formats license header by inserting the specified author for every occurrence of
    "{author}" in the header template.
    """
    return normalized_text(copyright_template.replace("{author}", author))


def is_commented_line(line: str, line_comments: list[str] = LINE_COMMENTS) -> bool:
    """Checks whether a line is a comment."""
    return line.lstrip().startswith(tuple(line_comments))


def is_empty_line(line: str) -> bool:
    """Checks whether a line is empty."""
    return not line.strip()


def get_header(file_path: Path, line_comments: list[str] = LINE_COMMENTS):
    """Extracts the header from a file and normalizes it."""
    header_lines: list[str] = []

    try:
        with open(file_path) as file:
            for line in file:
                if is_commented_line(
                    line, line_comments=line_comments
                ) or is_empty_line(line):
                    header_lines.append(line)
                else:
                    break
    except UnicodeDecodeError as error:
        raise UnexpectedBinaryFileError(file_path=file_path) from error

    # normalize the lines:
    header = "".join(header_lines)
    return normalized_text(header, line_comments=line_comments)


def validate_year_string(year_string: str, min_year: int = MIN_YEAR) -> bool:
    """Check if the specified year string is valid.
    Returns `True` if valid or `False` otherwise."""

    current_year = date.today().year

    # If the year_string is a single number, it must be the current year:
    if year_string.isnumeric():
        return int(year_string) == current_year

    # Otherwise, a range (e.g. 2021 - 2025) is expected:
    match = re.match(r"(\d+) - (\d+)", year_string)

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
    line_comments: list[str] = LINE_COMMENTS,
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
        header. This defaults to an author info for GHGA.

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
    exclude: list[str] = EXCLUDE,
    exclude_endings: list[str] = EXCLUDE_ENDINGS,
    exclude_pattern: list[str] = EXCLUDE_PATTERN,
    line_comments: list[str] = LINE_COMMENTS,
    min_year: int = MIN_YEAR,
) -> tuple[list[Path], list[Path]]:
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
        exclude (list[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (list[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (list[str], optional):
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
    passed_files: list[Path] = []
    failed_files: list[Path] = []

    for target_file in target_files:
        try:
            header = get_header(target_file, line_comments=line_comments)
            if check_copyright_notice(
                copyright=header,
                global_copyright=global_copyright,
                copyright_template=copyright_template,
                author=author,
                line_comments=line_comments,
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
    line_comments: list[str] = LINE_COMMENTS,
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

    with open(license_file) as file_:
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
        line_comments=line_comments,
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
    with open(GLOBAL_COPYRIGHT_FILE_PATH) as global_copyright_file:
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
