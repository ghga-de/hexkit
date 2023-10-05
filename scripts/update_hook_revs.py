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
"""Script to ensure the pre-commit hook revs match what is installed."""
import re
import sys
from functools import partial
from pathlib import Path

from packaging.requirements import Requirement

from script_utils import cli, lock_deps

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
PRE_COMMIT_CFG_PATH = REPO_ROOT_DIR / ".pre-commit-config.yaml"
LOCK_FILE_PATH = REPO_ROOT_DIR / "requirements-dev.txt"


def make_dependency_dict(requirements: list[Requirement]) -> dict[str, str]:
    """Accept a list of Requirement objects and convert to dict"""
    processed = {
        req.name: str(req.specifier).removeprefix("==") for req in requirements
    }

    return processed


def get_repl_value(match, dependencies: dict[str, str], outdated_hooks: list[str]):
    """Look up pre-commit hook id in list of dependencies. If there's a match, update
    `outdated_hooks` and return the hook version stored in the dictionary"""
    ver, name = match.groups()
    if name in dependencies:
        new_ver = dependencies[name].strip()

        # Use the v prefix if it was used before
        if ver.startswith("v"):
            new_ver = ver[0] + new_ver

        # Make a list of what's outdated
        if new_ver != ver:
            msg = f"\t{name} (configured: {ver}, expected: {new_ver})"
            outdated_hooks.append(msg)
        return new_ver
    return ver


def get_config():
    """Obtain the current pre-commit hook config from .pre-commit-config.yaml"""
    with open(PRE_COMMIT_CFG_PATH, encoding="utf-8") as pre_commit_config:
        return pre_commit_config.read()


def process_config(dependencies: dict[str, str], config: str) -> tuple[str, list[str]]:
    """Compare pre-commit config with lock file dependencies.

    Create a modified copy of the existing config file contents with the hook versions
    synchronized to the lock file dependencies.

    Returns:
        `new_config`: the updated/synchronized pre-commit config.

        `outdated_hooks`: a list of any outdated hooks with version discrepancy details.
    """
    outdated_hooks: list[str] = []

    hook_rev = re.compile(r"([^\s\n]+)(?=\s*hooks:\s*- id: ([^\s]+))")

    new_config = re.sub(
        hook_rev,
        repl=partial(
            get_repl_value, dependencies=dependencies, outdated_hooks=outdated_hooks
        ),
        string=config,
    )

    return new_config, outdated_hooks


def update_config(new_config: str):
    """Write `new_config` to .pre-commit-config.yaml"""
    with open(PRE_COMMIT_CFG_PATH, "w", encoding="utf-8") as pre_commit_config:
        pre_commit_config.write(new_config)
        cli.echo_success(f"Updated '{PRE_COMMIT_CFG_PATH}'")


def output_failure(outdated_hooks: list[str]):
    """Notify the user that some pre-commit hooks are outdated, and list those hooks."""
    cli.echo_failure("The following pre-commit hook versions are outdated:")
    for hook in outdated_hooks:
        print(hook)
    print("Run 'scripts/update_hook_revs.py' to update")
    sys.exit(1)


def main(check: bool = False):
    """Compare configured pre-commit hooks with the installed dependencies.

    For the set that overlap (e.g. `black`, `mypy`, `ruff`, etc.), make sure the
    versions match. If running with `--check`, exit with status code 1 if anything is
    outdated. If running without `--check`, update `.pre-commit-config.yaml` as needed.
    """

    dependencies: list[Requirement] = lock_deps.get_lock_file_deps(LOCK_FILE_PATH)
    dependency_dict: dict[str, str] = make_dependency_dict(dependencies)
    config = get_config()
    new_config, outdated_hooks = process_config(dependency_dict, config)

    if config != new_config:
        if check:
            output_failure(outdated_hooks)
        else:
            update_config(new_config)
    else:
        cli.echo_success("Pre-commit hooks are up to date.")


if __name__ == "__main__":
    cli.run(main)
