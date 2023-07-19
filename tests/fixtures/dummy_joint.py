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
"""Simple joint fixture for testing the event loop fixture override's impact"""

from dataclasses import dataclass
from typing import AsyncGenerator

import pytest_asyncio

from hexkit.providers.s3.testutils import S3Fixture, get_s3_fixture


@dataclass
class JointFixture:
    """Simplest joint fixture"""

    s3_fixture: S3Fixture


s3_fixture = get_s3_fixture("module")


@pytest_asyncio.fixture(scope="module")
async def joint_fixture(s3_fixture) -> AsyncGenerator[JointFixture, None]:
    yield JointFixture(s3_fixture=s3_fixture)
