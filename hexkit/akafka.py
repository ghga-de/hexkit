# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

"""Functionality for publishing or subscribing to a Kafka topic"""

import json
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import jsonschema
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseSettings

from .utils import OutOfContextError


class EventSchemaNotFoundError(RuntimeError):
    """Thrown when an event schema was not found."""

    pass  # pylint: disable=unnecessary-pass


class EventSchemaValidationError(RuntimeError):
    """Thrown when event schema was not valid."""

    def __init__(self, reason: str):
        """Create error with corresponding message."""
        message = f"Event failed schema validation: {reason}"
        super().__init__(message)


# pylint: disable=too-few-public-methods
class KafkaConfigBase(BaseSettings):
    """A base class with config params related to
    asynchronous messaging.
    Inherit your config class from this class if you need
    to run an async PubSub API."""

    service_name: str
    client_suffix: str

    kafka_servers: List[str]


def validate_payload(
    payload: dict, json_schema: dict, raise_on_exception: bool = False
) -> bool:
    """Validate an event payload based on the specified json_schema."""
    try:
        jsonschema.validate(instance=payload, schema=json_schema)
        return True
    except jsonschema.exceptions.ValidationError as exc:
        logging.error(
            "%s: Message payload does not comform to JSON schema.",
            datetime.now(timezone.utc).isoformat(),
        )
        logging.exception(exc)
        if raise_on_exception:
            raise exc
        return False


def process_func_factory(
    exec_funcs: Dict[str, Callable[[dict], None]], event_schemas: Dict[str, dict]
):
    """
    Returns a function for processing all events that are of interest.
    It validates the payload and selects the correct function for processing out of
    the exec_funcs dict.
    """

    def process_func_wrapper(event):
        """Adds validation to exec_on_event function"""

        if "type" not in event.value.keys() or "payload" not in event.value.keys():
            raise EventSchemaValidationError(
                reason='The event value must contain the fields "type" and "payload".'
            )

        event_type = event.value["type"]

        if event_type in exec_funcs.keys():  # else ignore event
            json_schema = event_schemas[event_type]
            event_payload = event.value["payload"]
            validate_payload(
                event_payload, json_schema=json_schema, raise_on_exception=True
            )

            exec_func = exec_funcs[event_type]
            exec_func(event_payload)

    return process_func_wrapper


# pylint: disable=too-few-public-methods
class EventClient:
    """A base class to connect and iteract to/with a Kafka host."""

    def __init__(
        self,
        config: KafkaConfigBase,
        event_schemas: Dict[str, dict],
    ):
        """Initialize the Producer.

        Args:
            config [KafkaConfigBase]:
                Config paramaters provided as KafkaConfigBase object.
            event_schemas (Dict[str, dict]):
                A dictionary where the keys are the event types (== event keys) and the
                values are schemas for the event payload (== event value).
        """
        self.bootstrap_servers = config.kafka_servers
        self.service_name = config.service_name
        self.client_suffix = config.client_suffix
        self.client_id = f"{self.service_name}.{self.client_suffix}"
        self.event_schemas = event_schemas


class EventConsumer(EventClient):
    "A client to consume events from Apache Kafka."

    def __init__(
        self,
        config: KafkaConfigBase,
        topic_names: List[str],
        exec_funcs: Dict[str, Callable[[dict], None]],
        event_schemas: Dict[str, dict],
    ):
        """
        Initialize the EventConsumer.

        Args:
            config [KafkaConfigBase]:
                Config paramaters provided as KafkaConfigBase object.
            topic_names (List[str]):
                A list of topic name to subscribe to (only use letters, numbers, and "_").
            exec_funcs (Dict[str, Callable[[dict], None]]):
                A dictionary where the keys are the event types (== event keys) and the
                values are the callables to process the corresponding events.
                The only argument of the callables is the event value (a dict).
            event_schemas (Dict[str, dict]):
                A dictionary where the keys are the event types (== event keys) and the
                values are schemas for the event payload (== event value).
        """
        super().__init__(config=config, event_schemas=event_schemas)
        self.topic_names = topic_names
        self.exec_funcs = exec_funcs

        for event_type in exec_funcs.keys():
            if event_type not in event_schemas.keys():
                raise EventSchemaNotFoundError(
                    "The event_schemas dictionary is missing the schema for the event"
                    + f' type "{event_type}"'
                )

        self._consumer: Optional[KafkaConsumer] = None

    def __enter__(self):
        """Initialize the Kafka Producer"""

        self._consumer = KafkaConsumer(
            *self.topic_names,
            client_id=self.client_id,
            group_id=self.service_name,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
        )

        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """
        Autocommit the current offset.
        """
        if not isinstance(self._consumer, KafkaConsumer):
            raise OutOfContextError()
        self._consumer.close(autocommit=True)

    def subscribe(self, run_forever: bool = True):
        """Subscribe to a topic and execute the specified function whenever
        a message is received.

        Args:
            run_forever (bool):
                If `True`, the function will continue to consume event for ever.
                If `False`, the function will wait for the first event, cosume it,
                and exit. Defaults to `True`.
        """

        if not isinstance(self._consumer, KafkaConsumer):
            raise OutOfContextError()

        process_event = process_func_factory(
            self.exec_funcs, event_schemas=self.event_schemas
        )

        if run_forever:
            for event in self._consumer:
                process_event(event)
        else:
            event = next(self._consumer)
            process_event(event)


class EventProducer(EventClient):
    "A client to publish events to Apache Kafka."

    def __init__(
        self,
        config: KafkaConfigBase,
        topic_name: str,
        event_schemas: Dict[str, dict],
    ):
        """Initialize the Producer.

        Args:
            config [KafkaConfigBase]:
                Config paramaters provided as KafkaConfigBase object.
            topic_name (str):
                The name of the topic (only use letters, numbers, and "_").
            event_schemas (Dict[str, dict]):
                A dictionary where the keys are the event types (== event keys) and the
                values are schemas for the event payload (== event value).
        """
        super().__init__(config=config, event_schemas=event_schemas)
        self.bootstrap_servers = config.kafka_servers
        self.service_name = config.service_name
        self.client_suffix = config.client_suffix
        self.client_id = f"{self.service_name}.{self.client_suffix}"
        self.topic_name = topic_name
        self.event_schemas = event_schemas

        self._producer: Optional[KafkaProducer] = None

    def __enter__(self):
        """Initialize the Kafka Producer"""

        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            key_serializer=lambda event_key: event_key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """
        Flush and close producer. Blocks until all published events are transmitted.
        """
        if not isinstance(self._producer, KafkaProducer):
            raise OutOfContextError()
        self._producer.flush()
        self._producer.close()

    def publish(self, event_type: str, event_key: str, event_payload: dict):
        """Publish a message to the topic

        Args:
            event_type (str):
                The type of event. A keyword string that describes the action. Event
                types that reflect the lifetime of a user account could be:
                "user_registered", "user_activated", "user_inactived", "user_deleted",
                etc.
            event_key (str):
                The event key as str. Usually the identified of the resource that this
                event is related to. E.g. in a "user_registered" event, the key could be
                the ID of the user (e.g. "user-012323456").
            event_payload (dict):
                The event/message payload as a dict. I.e. the information that should be
                transmitted with this event.
        """

        if not isinstance(self._producer, KafkaProducer):
            raise OutOfContextError()

        # validate payload against schema:
        if event_type not in self.event_schemas.keys():
            raise EventSchemaNotFoundError(
                f'Schema for event of type "{event_type}"'
                + " was not found in the specified event_schemas dict."
            )
        event_schema = self.event_schemas[event_type]
        validate_payload(
            event_payload, json_schema=event_schema, raise_on_exception=True
        )

        # construct the event value:
        event_value = {
            "type": event_type,
            "payload": event_payload,
        }

        self._producer.send(self.topic_name, key=event_key, value=event_value)
