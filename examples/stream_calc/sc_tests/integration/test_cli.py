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
#

"""Test the stream_calc app via the CLI."""

import asyncio
import os
import site
import subprocess
import sys
from pathlib import Path

import pytest

from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)
from sc_tests.integration.test_event_api import (
    CASES,
    check_problem_outcomes,
    submit_test_problems,
)

APP_DIR = Path(__file__).parent.parent.parent.absolute()


@pytest.mark.asyncio()
async def test_cli(kafka: KafkaFixture, monkeypatch: pytest.MonkeyPatch):
    """Test the stream_calc app via the CLI."""
    os.chdir(APP_DIR)
    kafka_server = kafka.kafka_servers[0]
    monkeypatch.setenv(name="STREAM_CALC_KAFKA_SERVERS", value=f'["{kafka_server}"]')
    monkeypatch.setenv(
        name="PYTHONPATH", value=":".join((str(APP_DIR), *site.getsitepackages()))
    )

    await submit_test_problems(CASES, kafka_server=kafka_server)

    with subprocess.Popen(
        args=["-m", "stream_calc"],
        executable=sys.executable,
    ) as process:
        await asyncio.wait_for(
            check_problem_outcomes(cases=CASES, kafka_server=kafka_server),
            10,
        )
        process.terminate()
