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
#
"""Tests for the log module."""

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import uuid4

import pytest
from pydantic import Field, SecretBytes, SecretStr

from hexkit.log import (
    JsonFormatter,
    LoggingConfig,
    RecordCompiler,
    configure_logging,
)
from tests.fixtures.utils import root_logger_reset  # noqa: F401

VALID_SERVICE_NAME = "test"
VALID_INSTANCE_ID = "001"
VALID_CONFIG = LoggingConfig(
    log_level="INFO",
    service_name="hexkit-test",
    service_instance_id="0",
)

KEYS_IN_JSON_LOG = (  # all the keys present in the resulting log
    "timestamp",
    "service",
    "instance",
    "level",
    "name",
    "correlation_id",
    "message",
    "details",
)

ADDED_KEYS = (  # a list of non-standard keys added by the RecordCompiler
    "timestamp",
    "service",
    "instance",
    "level",
    "correlation_id",
    "details",
)

DEFAULT_CONFIG = LoggingConfig(service_name="", service_instance_id="")


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

    def update_values_from_dict(self, values: dict[str, str]):
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
            loaded_log.update_values_from_dict(json.loads(captured))
        except json.JSONDecodeError as err:
            raise RuntimeError("Log doesn't seem to be in JSON format") from err

    return capture_log


def test_configured_log_level(caplog, expect_json_log):
    """Test with config"""
    logger = logging.getLogger("test_configured_log_level")
    config = LoggingConfig(
        log_level="CRITICAL",  # set to higher than default of WARNING
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format="",
    )

    configure_logging(config=config, logger=logger)

    assert not caplog.records

    # assert nothing was captured due to config
    logger.warning("should not log")
    assert not caplog.records

    # verify that the log is emitted and that it's the one we expect
    with expect_json_log() as json_log:
        logger.critical("should log")

    assert isinstance(json_log, JsonLog)
    assert json_log.service == config.service_name
    assert json_log.instance == config.service_instance_id
    assert json_log.message == "should log"


def test_record_compiler(caplog):
    """Unit test for the RecordCompiler class."""
    record_compiler = RecordCompiler(config=DEFAULT_CONFIG)

    assert not caplog.records
    log = logging.getLogger("test_record_compiler")
    log.setLevel("INFO")
    log.error("This is a test")

    assert len(caplog.records) == 1
    record = caplog.records[0]

    for key in ADDED_KEYS:
        assert key not in record.__dict__

    # Feed the record to the RecordCompiler
    record_compiler.handle(record)
    for key in KEYS_IN_JSON_LOG:
        assert key in record.__dict__


@pytest.mark.parametrize("exc_info", [False, True])
@pytest.mark.parametrize("exc_text", [False, True])
@pytest.mark.parametrize("include_traceback", [False, True])
def test_json_formatter(exc_info, exc_text, include_traceback, caplog):
    """Test that the JsonFormatter works like expected"""
    formatter = JsonFormatter(include_traceback=include_traceback)

    assert not caplog.records
    log = logging.getLogger("test_json_formatter")
    log.setLevel("INFO")
    try:
        raise ValueError("This is a test exception")
    except ValueError:
        log.error(
            "This is a %s",
            "test",  # test with arg to make sure this isn't lost
            exc_info=exc_info,
        )

    assert len(caplog.records) == 1
    record = caplog.records[0]

    # we get an error if we skip the RecordCompiler
    with pytest.raises(KeyError):
        formatter.format(record)

    # use the record compiler to stick in the extra fields
    record_compiler = RecordCompiler(config=DEFAULT_CONFIG)
    record_compiler.handle(record)

    assert bool(record.exc_text) == exc_info
    if not exc_text:
        # remove the cached traceback for this test
        record.exc_text = None

    # format with the json formatter
    output = formatter.format(record)

    # load back into json format and verify the added keys are there
    json_log = json.loads(output)
    for key in ADDED_KEYS:
        assert key in json_log

    if exc_info:
        assert "exception" in json_log
        exception = json_log["exception"]
        assert isinstance(exception, dict)
        assert "type" in exception
        assert exception.pop("type") == "ValueError"
        assert "message" in json_log
        assert exception.pop("message") == "This is a test exception"
        if include_traceback:
            assert "traceback" in exception
            tb = exception.pop("traceback")
            assert tb
            assert "Traceback (most recent call last):" in tb
            assert "File" in tb
            assert "tests/unit/test_logging.p" in tb
            assert "line" in tb
            assert "in test_json_formatter" in tb
            assert 'raise ValueError("This is a test exception")' in tb
            assert "ValueError: This is a test exception" in tb
            assert tb.count("\n") == 3
        assert not exception  # no other properties should be present
    else:
        assert "exception" not in json_log

    # make sure the message is what we expected
    assert json_log["message"] == "This is a test"


@pytest.mark.parametrize("log_traceback", [False, True])
def test_configured_traceback(log_traceback, caplog, expect_json_log):
    """Test configuring the traceback via the config."""
    log = logging.getLogger(f"test_configured_traceback_{log_traceback}")
    config = LoggingConfig(
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_traceback=log_traceback,
    )
    configure_logging(config=config, logger=log)

    handlers = log.handlers
    assert len(handlers) == 1
    formatter = handlers[0].formatter
    assert isinstance(formatter, JsonFormatter)
    assert formatter._include_traceback == log_traceback


@pytest.mark.parametrize(
    "log_format, formatter_class",
    [
        (None, JsonFormatter),
        ("%(message)s", logging.Formatter),
    ],
)
def test_formatter_selection(
    log_format: Optional[str], formatter_class: type[logging.Formatter]
):
    """Make sure the proper formatter is selected based on the config."""
    log = logging.getLogger(f"test_formatter_selection_{log_format is None}")
    config = LoggingConfig(
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format=log_format,
    )
    configure_logging(config=config, logger=log)

    handlers = log.handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0].formatter, formatter_class)


def test_reconfiguration_of_existing_loggers():
    """Ensure applying configuration to an existing logger works as expected."""
    log = logging.getLogger("reconfig")
    trace = 5

    assert len(log.handlers) == 0

    config = LoggingConfig(
        log_level="TRACE",  # can verify as well that the custom level exists
        service_name=VALID_SERVICE_NAME,
        service_instance_id=VALID_INSTANCE_ID,
        log_format="%(timestamp)s - %(msg)s",
    )

    configure_logging(config=config, logger=log)
    assert log.getEffectiveLevel() == trace
    assert isinstance(log.handlers[0].formatter, logging.Formatter)


def test_root_logger_configuration(root_logger_reset):  # noqa: F811
    """Test that `configure_logging` configures the root logger by default.

    In case of failure, the fixture should prevent leaving root logger in modified state.
    """
    root = logging.getLogger()

    # Verify that no RecordCompiler handlers exist
    for handler in root.handlers:
        assert not isinstance(handler, RecordCompiler)

    # Configure and retrieve copy of list of handlers post-configuration
    configure_logging(config=DEFAULT_CONFIG)
    root_handlers = root.handlers.copy()
    level = root.level

    # Now perform check to see if RecordCompiler was actually added to
    assert level == 20  # INFO equates to 20 by default
    assert any(isinstance(handler, RecordCompiler) for handler in root_handlers)


def test_secrets_logging_config(root_logger_reset, capsys):  # noqa: F811
    """Make sure secret values are not revealed when logging the configuration.

    Additionally, make sure all other values *are* logged.
    """

    class TestConfig(LoggingConfig):
        string: str = "foo"
        number: int = 42
        secret_bytes: SecretBytes = Field(default=SecretBytes(b"silent"))
        secret_str: SecretStr = Field(default=SecretStr("silent"))

    config = TestConfig(service_name="", service_instance_id="")

    configure_logging(config=config)
    out, err = capsys.readouterr()
    printed_log = out + err

    assert "Logging configured, complete configuration in details" in printed_log
    assert "silent" not in printed_log
    for key, value in config.model_dump().items():
        assert key in printed_log
        if not key.startswith("secret"):
            json_value = json.dumps(value)
            assert json_value in printed_log, printed_log


def test_complex_types_in_log_detail(root_logger_reset, capsys):  # noqa: F811
    """Test that complex types in log details are serialized with repr."""
    config = LoggingConfig(service_name="", service_instance_id="")
    configure_logging(config=config)

    out, err = capsys.readouterr()
    printed_log = out + err
    test_uuid = uuid4()
    date = datetime.now()

    log = logging.getLogger()
    log.error("Testing", extra={"id_": test_uuid, "date": date})
    out, err = capsys.readouterr()
    printed_log = out + err

    assert "Testing" in printed_log
    assert repr(test_uuid) in printed_log
    assert repr(date) in printed_log
