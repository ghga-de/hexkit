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
"""Tests for the log_tools module."""

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest

from hexkit.log_tools import (
    JsonFormatter,
    LoggerFactory,
    LoggingConfig,
    RecordCompiler,
    StructuredLogger,
)

VALID_SERVICE_NAME = "test"
VALID_INSTANCE_ID = "001"
VALID_CONFIG = LoggingConfig(
    log_level="INFO",
    service_name="hexkit-test",
    service_instance_id="0",
)

DEFAULT_KEYS = (  # all the keys present in the resulting log
    "timestamp",
    "service",
    "instance",
    "level",
    "name",
    "correlation_id",
    "message",
    "details",
)

ADDED_KEYS = (  # a list of non-standard keys added by the adapter/record compiler
    "timestamp",
    "service",
    "instance",
    "level",
    "correlation_id",
    "details",
)

DEFAULT_CONFIG = LoggingConfig(service_name="", service_instance_id="")


@pytest.fixture(autouse=True)
def reset_config():
    """Reset the config before and after each test."""
    LoggerFactory.configure(log_config=DEFAULT_CONFIG)
    yield
    LoggerFactory.configure(log_config=DEFAULT_CONFIG)


@dataclass
class JsonLog:
    """A class to represent the Json Log format for convenience"""

    timestamp: str = ""
    service: str = ""
    instance: str = ""
    level: str = ""
    name: str = ""
    correlation_id: str = ""
    message: str = ""
    details: dict[str, str] = field(default_factory=dict)

    def get_values_from_dict(self, values: dict[str, str]):
        """Modify dataclass in place"""
        self.__dict__.update(values)


@pytest.fixture
def expect_json_log(capsys):
    """Fixture used to read logs from stderr and parse them as JSON for examination.

    Raises:
        - RuntimeError: when the log isn't in JSON format.
    """

    @contextmanager
    def capture_log():
        """Context manager to clear the bugger, capture input, and construct a JsonLog."""
        capsys.readouterr()  # Clear the capture buffer
        loaded_log: JsonLog = JsonLog()
        yield loaded_log
        captured = capsys.readouterr().err  # Capture the stderr
        try:
            loaded_log.get_values_from_dict(json.loads(captured))
        except json.JSONDecodeError as err:
            raise RuntimeError("Log doesn't seem to be in JSON format") from err

    return capture_log


def test_logger_construction():
    """Make sure the factory returns the correct class"""
    logger = LoggerFactory.get_configured_logger("test_logger_construction")
    assert isinstance(logger, StructuredLogger)
    assert isinstance(logger.logger, logging.Logger)
    handlers = logger.logger.handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], RecordCompiler)
    assert isinstance(handlers[0].formatter, JsonFormatter)


def test_no_config(caplog, expect_json_log):
    """Make sure you can still log without configuration."""
    logger = LoggerFactory.get_configured_logger("test_no_config")
    assert not caplog.records

    logger.debug("should not log")
    assert not caplog.records

    with expect_json_log() as json_log:
        logger.info("should log")

    assert isinstance(json_log, JsonLog)  # for autocompletion
    assert json_log.level == "INFO"
    assert json_log.service == ""
    assert json_log.instance == ""
    assert json_log.message == "should log"

    with expect_json_log() as thing:
        logger.error("should log too")
    assert thing.message == "should log too"
    assert thing.level == "ERROR"


def test_with_config(expect_json_log):
    """Test with config"""
    config = LoggingConfig(
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format="",
    )

    LoggerFactory.configure(log_config=config)

    log = LoggerFactory.get_configured_logger("test_with_config")

    with expect_json_log() as json_log:
        log.error("hi")

    assert isinstance(json_log, JsonLog)
    assert json_log.service == config.service_name
    assert json_log.instance == config.service_instance_id
    assert json_log.message == "hi"


def test_record_compiler(caplog):
    """Unit test for the RecordCompiler class."""
    record_compiler = RecordCompiler()

    assert not caplog.records
    log = logging.getLogger("hi")  # just get root logger
    log.setLevel("INFO")
    log.error("This is a test")

    assert len(caplog.records) == 1
    record = caplog.records[0]

    for key in ADDED_KEYS:
        assert key not in record.__dict__

    # Feed the record to the RecordCompiler
    record_compiler.handle(record)
    for key in DEFAULT_KEYS:
        assert key in record.__dict__


def test_json_formatter(caplog):
    """Test that the JsonFormatter works like expected"""
    formatter = JsonFormatter()

    assert not caplog.records
    log = logging.getLogger()  # just get root logger
    log.setLevel("INFO")
    log.error("This is a %s", "test")  # test with arg to make sure this isn't lost

    assert len(caplog.records) == 1
    record = caplog.records[0]

    # we get an error if we skip the RecordCompiler
    with pytest.raises(KeyError):
        formatter.format(record)

    # use the record compiler to stick in the extra fields
    record_compiler = RecordCompiler()
    record_compiler.handle(record)

    # format with the json formatter
    output = formatter.format(record)

    # load back into json format and verify the expected keys are all there
    json_log = json.loads(output)
    for key in DEFAULT_KEYS:
        assert key in json_log

    # make sure the message is what we expected
    assert json_log["message"] == "This is a test"


@pytest.mark.parametrize(
    "log_format, formatter_class",
    [
        ("", JsonFormatter),
        ("%(message)s", logging.Formatter),
    ],
)
def test_formatter_selection(log_format: str, formatter_class: type[logging.Formatter]):
    """Make sure the proper formatter is selected based on the config."""
    config = LoggingConfig(
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format=log_format,
    )

    LoggerFactory.configure(log_config=config)

    log = LoggerFactory.get_configured_logger("test_formatter_selection")

    handlers = log.logger.handlers
    assert isinstance(handlers[0].formatter, formatter_class)


def test_reconfiguration_of_existing_loggers():
    """Ensure applying configuration to the LoggerFactory updates all existing loggers
    as well as loggers created thereafter.
    """
    log = LoggerFactory.get_configured_logger("reconfig")
    log2 = LoggerFactory.get_configured_logger("reconfig2")
    info = 20
    trace = 5

    assert log.getEffectiveLevel() == info
    assert log2.getEffectiveLevel() == info
    assert isinstance(log.logger.handlers[0].formatter, JsonFormatter)
    assert isinstance(log2.logger.handlers[0].formatter, JsonFormatter)

    config = LoggingConfig(
        log_level="TRACE",  # can verify as well that the custom level exists
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format="%(timestamp)s - %(msg)s",
    )

    LoggerFactory.configure(log_config=config)
    assert log.getEffectiveLevel() == trace
    assert log2.getEffectiveLevel() == trace
    assert isinstance(log.logger.handlers[0].formatter, logging.Formatter)
    assert isinstance(log2.logger.handlers[0].formatter, logging.Formatter)

    log3 = LoggerFactory.get_configured_logger("reconfig3")
    assert log3.getEffectiveLevel() == trace
    assert isinstance(log3.logger.handlers[0].formatter, logging.Formatter)
