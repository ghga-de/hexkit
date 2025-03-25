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

"""Configurable logging tools with JSON output."""

import json
from datetime import datetime, timezone
from logging import Formatter, Logger, LogRecord, StreamHandler, addLevelName, getLogger
from typing import Any, Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings

from hexkit.correlation import (
    correlation_id_var,
)

__all__ = [
    "JsonFormatter",
    "LoggingConfig",
    "RecordCompiler",
    "configure_logging",
]


DUMMY_RECORD = LogRecord("dummy", 0, "dummy", 0, None, None, None, None, None)
RESERVED_RECORD_KEYS = set(DUMMY_RECORD.__dict__)

# Add TRACE log level
addLevelName(5, "TRACE")
LogLevel = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"]


class LoggingConfig(BaseSettings):
    """A class containing logging config."""

    log_level: LogLevel = Field(
        default="INFO", description="The minimum log level to capture."
    )
    service_name: str = Field(
        ...,
        examples=["my-cool-special-service"],
        description="The name of the (micro-)service. This will be included in log messages.",
    )
    service_instance_id: str = Field(
        ...,
        examples=["germany-bw-instance-001"],
        description=(
            "A string that uniquely identifies this instance across all instances of"
            + " this service. This is included in log messages."
        ),
    )
    log_format: Optional[str] = Field(
        default=None,
        examples=[
            "%(timestamp)s - %(service)s - %(level)s - %(message)s",
            "%(asctime)s - Severity: %(levelno)s - %(msg)s",
        ],
        description=(
            "If set, will replace JSON formatting with the specified string format. If"
            + " not set, has no effect. In addition to the standard attributes, the"
            + " following can also be specified: timestamp, service, instance, level,"
            + " correlation_id, and details"
        ),
    )
    log_traceback: bool = Field(
        default=True,
        description="Whether to include exception tracebacks in log messages.",
    )


class JsonFormatter(Formatter):
    """A formatter class that outputs logs in JSON format."""

    def __init__(self, include_traceback=True, *args, **kwargs):
        """Initialize the formatter."""
        super().__init__(*args, **kwargs)
        self._include_traceback = include_traceback

    def format(self, record: LogRecord) -> str:
        """Format the specified record as a JSON string.

        This will format the log record as JSON with the following values (in order):
            - timestamp: The ISO 8601-formatted timestamp of the log message.
            - service: The name of the service where the log was generated.
            - instance: The instance ID of the service where the log was generated.
            - level: The log's severity.
            - name: The name of the logger.
            - correlation_id: The correlation ID, if set, from the current context.
            - message: The message that was logged, formatted with any arguments.
            - details: Any additional values included at time of logging.
            - exception: If an exception was logged, the exception info.
        """
        # Create a log record dictionary
        log_record = record.__dict__
        output: dict[str, Any] = dict()

        output["timestamp"] = log_record["timestamp"]
        output["service"] = log_record["service"]
        output["instance"] = log_record["instance"]
        output["level"] = log_record["level"]
        output["name"] = log_record["name"]
        output["correlation_id"] = log_record["correlation_id"]
        output["message"] = record.getMessage()  # construct msg str with any args
        output["details"] = log_record["details"]

        if log_record["exc_info"]:
            exc_info = log_record["exc_info"]
            exc_type, exc_value = exc_info[:2]
            exception = {
                "type": exc_type.__name__,
                "message": str(exc_value),
            }
            if self._include_traceback:
                exc_text = log_record["exc_text"]
                if not exc_text:
                    # if there is not pre-formatted cached traceback, create it
                    exc_text = super().formatException(exc_info)
                if exc_text:
                    exception["traceback"] = exc_text
            output["exception"] = exception

        # Convert to JSON string
        return json.dumps(output)


class RecordCompiler(StreamHandler):
    """A class to make all non-standard information available to formatters."""

    def __init__(self, *, config: LoggingConfig):
        """Initialize with logging config."""
        super().__init__()

        self._service_name = config.service_name
        self._service_instance_id = config.service_instance_id

    def handle(self, record: LogRecord) -> bool:
        """Set custom record attributes"""
        log_record = record.__dict__
        timestamp = datetime.fromtimestamp(log_record["created"])
        timestamp = timestamp.astimezone(timezone.utc)
        iso_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        extras = {
            key: value
            for key, value in log_record.items()
            if key not in RESERVED_RECORD_KEYS
        }
        record.timestamp = iso_timestamp
        record.service = self._service_name
        record.instance = self._service_instance_id
        record.level = log_record["levelname"]
        record.correlation_id = correlation_id_var.get(None)
        record.details = extras
        return super().handle(record)


def configure_logging(*, config: LoggingConfig, logger: Optional[Logger] = None):
    """Set up logging.

    Configures the root logger by default, but can be used to configure a specific
    logger as well. Will also log the complete configuration of the service with
    secret values hidden, if it's passed to this function and inherits from LoggingConfig.
    """
    formatter = (
        Formatter(config.log_format)
        if config.log_format
        else JsonFormatter(include_traceback=config.log_traceback)
    )

    handler = RecordCompiler(config=config)
    handler.setLevel(config.log_level)
    handler.setFormatter(formatter)

    if not logger:
        logger = getLogger()

    logger.setLevel(config.log_level)
    logger.addHandler(handler)

    logger.info(
        "Logging configured, complete configuration in details",
        extra=config.model_dump(mode="json"),
    )
