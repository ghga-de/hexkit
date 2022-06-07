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

"""Implement global logic for running the application."""

import asyncio
import logging

from stream_calc.config import Config
from stream_calc.container import Container  # type: ignore


async def main() -> None:
    """Coroutine to run the stream calculator"""
    config = Config()

    logging.basicConfig(level=config.log_level)

    container = Container()
    container.config.from_pydantic(config)

    async with container as cont_context:
        event_subscriber = await cont_context.event_subscriber()
        await event_subscriber.run()


def run() -> None:
    """Run the main function."""
    asyncio.run(main())
