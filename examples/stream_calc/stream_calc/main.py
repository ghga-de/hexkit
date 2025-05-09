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

"""Implement global logic for running the application."""

import asyncio
import logging

from stream_calc.config import Config
from stream_calc.inject import prepare_event_subscriber


async def main(
    *,
    config: Config = Config(),
    run_forever: bool = True,
) -> None:
    """
    Coroutine to run the stream calculator.

    Args:
        config:
            App config object. Defaults to using the standard config constructor.
        run_forever:
            If set too `False`, will exit after handling one arithmetic problem.
    """
    logging.basicConfig(level=config.log_level)

    async with prepare_event_subscriber(config=config) as event_subscriber:
        await event_subscriber.run(forever=run_forever)


def run() -> None:
    """Run the main function."""
    asyncio.run(main())
