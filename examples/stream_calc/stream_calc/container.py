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

# pylint: disable=wrong-import-order
from stream_calc.config import Config
from stream_calc.core.calc import StreamCalculator
from stream_calc.translators.eventpub import EventResultEmitter
from stream_calc.translators.eventsub import EventProblemReceiver

from hexkit.inject import ContainerBase, get_configurator, get_constructor
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber


class Container(ContainerBase):
    """DI Container"""

    config = get_configurator(Config)

    # outbound providers:
    event_publisher = get_constructor(
        KafkaEventPublisher,
        config=config,
    )

    # outbound translators:
    event_result_emitter = get_constructor(
        EventResultEmitter,
        config=config,
        event_publisher=event_publisher,
    )

    # inbound ports:
    problem_handler = get_constructor(
        StreamCalculator, result_emitter=event_result_emitter
    )

    # inbound translators:
    event_problem_receiver = get_constructor(
        EventProblemReceiver,
        config=config,
        problem_handler=problem_handler,
    )

    # inbound providers:
    event_subscriber = get_constructor(
        KafkaEventSubscriber,
        config=config,
        translator=event_problem_receiver,
    )
