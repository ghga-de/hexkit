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

"""Configurable logging tools with JSON output."""

import json
from collections import OrderedDict
from collections.abc import MutableMapping
from datetime import datetime, timezone
from logging import (
    Formatter,
    LoggerAdapter,
    LogRecord,
    StreamHandler,
    addLevelName,
    getLogger,
)
from typing import Any, Literal

from pydantic import Field
from pydantic_settings import BaseSettings

from hexkit.correlation import (
    correlation_id_var,
)

__all__ = [
    "LoggingConfig",
    "StructuredLogger",
    "LoggerFactory",
    "configure_logging",
    "JsonFormatter",
]

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


def configure_logging(*, config: LoggingConfig):
    """Set up logging"""
    LoggerFactory.configure(
        log_config=config,
    )


class JsonFormatter(Formatter):
    """A formatter class that outputs logs in JSON format."""

    def format(self, record: LogRecord) -> str:
        """Format the specified record as text.

        This will format the log record as JSON with the following values (in order):
            - timestamp: The ISO 8601-formatted timestamp of the log message.
            - service: The name of the service where the log was generated.
            - instance: The instance ID of the service where the log was generated.
            - level: The log's severity.
            - name: The name of the logger.
            - correlation_id: The correlation ID, if set, from the current context.
            - message: The message that was logged, formatted with any arguments.
            - details: Any additional values included at time of logging.
        """
        # Create a log record dictionary
        log_record = record.__dict__
        output: OrderedDict[str, str] = OrderedDict()

        # Format to ISO 8601 with three decimal places for seconds
        timestamp = datetime.fromtimestamp(log_record["created"])
        timestamp = timestamp.astimezone(timezone.utc)
        iso_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        output["timestamp"] = iso_timestamp
        output["service"] = log_record.get("service", "Not set")
        output["instance"] = log_record.get("instance", "Not set")
        output["level"] = log_record["levelname"]
        output["name"] = log_record["name"]
        output["correlation_id"] = log_record.get("correlation_id", "")
        output["message"] = record.getMessage()  # construct msg str with any args
        output["details"] = log_record.get("details", {})

        # Convert to JSON string
        return json.dumps(output)


class StructuredLogger(LoggerAdapter):
    """Custom LoggerAdapter to add contextual information.

    Add correlation ID, service, instance, and places 'extra' param values into 'details'.
    """

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Process the logging message and keyword arguments passed in to a logging call
        to insert contextual information.

        This is where contextual information is added.
        Note: contextual in this case does not refer to ContextVars, although some
        information may be retrieved that way.
        """
        details = kwargs.pop("extra", {})
        kwargs["extra"] = {"details": details}
        kwargs["extra"]["correlation_id"] = correlation_id_var.get("")

        # Include 'service' and 'instance'
        if self.extra:
            kwargs["extra"].update({key: val for key, val in self.extra.items()})

        return msg, kwargs


class LoggerFactory:
    """A class that can take `LogConfig` and produce configured loggers accordingly.

    Usage:

    In main top-level module:
        ```
        from hexkit.log_tools import configure_logging
        config = Config()  # ensure it subclasses LoggingConfig
        configure_logging(config=config)
        ```
    In another module:
        ```
        from hexkit.log_tools import LoggerFactory
        log = LoggerFactory.get_configured_logger(__name__)
        log.error("The file with ID '%s' is invalid", file_id, extra={"file_id": file_id})
        ```
    """

    config: LoggingConfig = LoggingConfig(service_name="", service_instance_id="")
    _loggers: dict[str, StructuredLogger] = {}

    @classmethod
    def configure(
        cls,
        *,
        log_config: LoggingConfig,
    ):
        """Set configuration values and update any existing `StructuredLogger` objects.

        Will update existing loggers/logger adapters with config changes.

        Args:
            - `log_config`: Configuration used to set the log level and any other items.
        """
        cls.config = log_config
        extras = {
            "service": cls.config.service_name,
            "instance": cls.config.service_instance_id,
        }

        # Update registered logger adapters (StructuredLogger)
        for name in cls._loggers:
            cls._loggers[name].logger.setLevel(log_config.log_level.upper())

            if cls._loggers[name].extra != extras:
                cls._loggers[name].extra = extras

    @classmethod
    def get_configured_logger(cls, name: str) -> LoggerAdapter:
        """Returns a configured logger object with the provided name.

        Creates a new logger or returns an existing one if possible.
        Use the `extra` keyword in log calls to include information in the `details`
        field of the log message.
        """
        if name in cls._loggers:
            return cls._loggers[name]

        logger = getLogger(name)

        logger.setLevel(cls.config.log_level)

        handler = StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

        logger_adapter = StructuredLogger(
            logger,
            {
                "service": cls.config.service_name,
                "instance": cls.config.service_instance_id,
            },
        )
        cls._loggers[name] = logger_adapter

        return logger_adapter
