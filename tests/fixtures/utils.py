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
from typing import Literal

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


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"]


def assert_logged(
    level: LogLevel,
    message: str,
    records: list[logging.LogRecord],
    parse: bool = True,
) -> str:
    """Assert that a log message was logged with the given level and message.

    Args:
    - `level`: The log level to check for.
    - `message`: The message to check for.
    - `records`: The log records to check (usually `caplog.records`)
    - `parse`: Whether to parse the message with the arguments from the log record.

    If a match is found, the parsed message is returned (regardless of the value of
    `parse`) and removed from the records list.
    """
    for i, record in enumerate(records):
        msg_to_inspect = str(record.msg) % record.args if parse else record.msg
        if record.levelname == level and msg_to_inspect == message:
            del records[i]
            return msg_to_inspect if parse else str(record.msg) % record.args
    else:
        assert False, f"Log message not found: {level} - {message}"


def assert_not_logged(
    level: LogLevel,
    message: str,
    records: list[logging.LogRecord],
    parse: bool = True,
):
    """Assert that a log message was not logged with the given level and message.

    Args:
    - `level`: The log level to check for.
    - `message`: The message to check for.
    - `records`: The log records to check (usually `caplog.records`)
    - `parse`: Whether to parse the message with the arguments from the log record.
    """
    for record in records:
        msg_to_inspect = str(record.msg) % record.args if parse else record.msg
        if record.levelname == level and msg_to_inspect == message:
            assert False, f"Log message found: {level} - {message}"


@pytest.fixture(name="caplog_debug")
def caplog_debug_fixture(caplog):
    """Convenience fixture to set the log level of caplog to debug for a test."""
    caplog.set_level(logging.DEBUG)
    yield caplog
