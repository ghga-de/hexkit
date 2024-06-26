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

"""Utils for fixture handling"""

import logging
import os
from pathlib import Path

import pytest
import yaml

TEST_FILE_DIR = Path(__file__).parent.resolve() / "test_files"

TEST_FILE_PATHS = [
    TEST_FILE_DIR / filename
    for filename in os.listdir(TEST_FILE_DIR)
    if filename.startswith("test_") and filename.endswith(".yaml")
]


def read_yaml(path: Path) -> dict:
    """Read yaml file and return content as dict."""
    with open(path, encoding="UTF-8") as file:
        return yaml.safe_load(file)


@pytest.fixture
def root_logger_reset():
    """Reset root logger level and handlers after modification."""
    root = logging.getLogger()
    original_level = root.level
    root_handlers = root.handlers.copy()

    yield

    # reset level and handlers
    root.setLevel(original_level)
    root.handlers = root_handlers
