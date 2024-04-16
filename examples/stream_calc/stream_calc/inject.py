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

"""Module hosting the dependency injection container."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from stream_calc.config import Config
from stream_calc.core.calc import StreamCalculator
from stream_calc.ports.problem_receiver import ArithProblemHandlerPort
from stream_calc.translators.eventpub import EventResultEmitter
from stream_calc.translators.eventsub import EventProblemReceiver


@asynccontextmanager
async def prepare_core(
    *,
    config: Config,
) -> AsyncGenerator[ArithProblemHandlerPort, None]:
    """Constructs and initializes all core components and their outbound dependencies."""

    async with KafkaEventPublisher.construct(config=config) as event_pub_provider:
        result_emitter = EventResultEmitter(
            config=config, event_publisher=event_pub_provider
        )

        yield StreamCalculator(result_emitter=result_emitter)


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
) -> AsyncGenerator[KafkaEventSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies."""
    async with prepare_core(config=config) as stream_calculator:
        event_problem_receiver = EventProblemReceiver(
            config=config, problem_handler=stream_calculator
        )

        async with KafkaEventSubscriber.construct(
            config=config, translator=event_problem_receiver
        ) as event_subscriber:
            yield event_subscriber
