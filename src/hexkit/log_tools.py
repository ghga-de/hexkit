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

"""Tools for logging with hexkit."""

import json
from collections.abc import MutableMapping
from datetime import datetime, timezone
from logging import (
    Formatter,
    LoggerAdapter,
    LogRecord,
    StreamHandler,
    getLogger,
)
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings

from hexkit.correlation import (
    correlation_id_var,
)


class LoggingConfig(BaseSettings):
    """A class containing logging config."""

    log_level: str = Field(
        default="INFO", description="The minimum log level to capture."
    )


class JsonFormatter(Formatter):
    """A formatter class that outputs logs in JSON format."""

    def format(self, record: LogRecord):
        """Format the specified record as text.

        This will format the log record as JSON.
        """
        # Create a log record dictionary
        log_record = record.__dict__

        # Format to ISO 8601 with three decimal places for seconds
        timestamp = datetime.fromtimestamp(log_record["created"])
        timestamp = timestamp.astimezone(timezone.utc)
        iso_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        formatted = {}
        formatted["timestamp"] = iso_timestamp
        formatted["service"] = log_record.get("service", "Unknown")
        formatted["instance"] = log_record.get("instance", "Unknown")
        formatted["name"] = log_record["name"].upper()
        formatted["level"] = log_record["levelname"]
        formatted["message"] = record.getMessage()
        formatted["details"] = log_record["details"]

        # Convert to JSON
        return json.dumps(formatted)


class Adapter(LoggerAdapter):
    """Custom LoggerAdapter to add contextual information."""

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Process the logging message and keyword arguments passed in to a logging call
        to insert contextual information.

        This is where universal contextual information is added.
        """
        # Add extra arguments to 'details' key
        details = kwargs.pop("extra", {})
        kwargs["extra"] = {"details": details}
        kwargs["extra"]["correlation_id"] = correlation_id_var.get("")
        if self.extra:
            kwargs["extra"]["service"] = self.extra["service"]
            kwargs["extra"]["instance"] = self.extra["instance"]

        return msg, kwargs


class LoggerFactory:
    """A class that can take `LogConfig` and produce logger objects accordingly.

    Usage:

    In main top-level module:
        ```
        config = LogConfig()
        logger_factory = LoggerFactory(
            config=config,
            service_name="ucs",
            service_instance_id="1",
        )
        ```
    In another module:
        ```
        from main_module import logger_factory
        log = logger_factory.get_configured_logger(name=__name__)
        log.info("The file with ID '%s' is invalid", file_id, extra={"file_id": file_id})
        ```
    """

    def __init__(
        self,
        *,
        log_config: LoggingConfig,
        service_name: str,
        service_instance_id: str,
    ):
        self._config = log_config
        self._service_name = service_name
        self._service_instance_id = service_instance_id

    def get_configured_logger(self, *, name: str) -> LoggerAdapter:
        """Returns a configured logger object with the provided name."""
        logger = getLogger(name)
        logger.setLevel(self._config.log_level)

        handler = StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

        logger_adapter = Adapter(
            logger,
            {
                "service": self._service_name,
                "instance": self._service_instance_id,
            },
        )

        return logger_adapter
