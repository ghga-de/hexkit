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
#

# When mypy is started from pre-commit, it exits with an exception when analysing this
# file without useful error message, thus ignoring.
# (Please note this does not occur when running mypy directly.)
#  type: ignore

"""Module hosting the dependency injection container."""

from dependency_injector import containers, providers

# pylint: disable=wrong-import-order
from stream_calc.core.calc import StreamCalculator

from examples.stream_calc.ports.problem_receiver import ArithProblemReceiverPort
from examples.stream_calc.tanslators.eventpub import EventResultEmitter
from examples.stream_calc.tanslators.eventsub import EventProblemReceiver
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber


class Container(containers.DeclarativeContainer):
    """DI Container"""

    config = providers.Configuration()

    # outbound providers:
    event_publisher = providers.Factory[EventPublisherProtocol](
        KafkaEventPublisher,
        service_name=config.service_name,
        client_suffix=config.client_suffix,
        kafka_servers=config.kafka_servers,
    )

    # outbound translators:
    event_result_emitter = providers.Factory[EventResultEmitter](
        EventResultEmitter, event_publisher=event_publisher
    )

    # inbound ports:
    problem_receiver = providers.Factory[ArithProblemReceiverPort](
        StreamCalculator, result_emitter=event_result_emitter
    )

    # inbound translators:
    event_problem_receiver = providers.Factory[EventSubscriberProtocol](
        EventProblemReceiver, problem_receiver=problem_receiver
    )

    # inbound providers:
    event_subscriber = providers.Factory[KafkaEventSubscriber](
        KafkaEventSubscriber,
        service_name=config.service_name,
        client_suffix=config.client_suffix,
        kafka_servers=config.kafka_servers,
        translator=event_problem_receiver,
    )
