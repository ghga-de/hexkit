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

"""Module hosting the dependency injection container."""

from dependency_injector import containers, providers

# pylint: disable=wrong-import-order
from stream_calc.core.calc import StreamCalculator
from stream_calc.ports.inbound.stream_calculator import StreamCalculatorPort
from stream_calc.tanslators.outbound.eventpub import EventResultEmitter

from hexkit.eventpub.protocol import EventPublisherProto
from hexkit.eventpub.providers.akafka import KafkaEventPublisher


class Container(containers.DeclarativeContainer):
    """DI Container"""

    config = providers.Configuration()

    # outbound providers:
    event_publisher = providers.Factory[EventPublisherProto](
        KafkaEventPublisher,
        service_name=config.service_name,
        client_suffix=config.client_suffix,
        kafka_servers=config.kafka_servers,
    )

    # outbound translators:
    result_emitter = providers.Factory[EventResultEmitter](
        EventResultEmitter, event_publisher=event_publisher
    )

    # outbound ports:
    stream_calculator = providers.Factory[StreamCalculatorPort](
        StreamCalculator, result_emitter=result_emitter
    )
