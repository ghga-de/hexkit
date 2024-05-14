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

"""Fixtures for integration tests that reuse the same test container."""

import asyncio

import pytest

from hexkit.providers.s3.testutils import (
    S3Fixture,
    file_fixture,
    get_s3_fixture,
)

__all__ = [
    "s3_fixture",
    "file_fixture",
]

s3_session_fixture = get_s3_fixture(scope="session")


@pytest.fixture(name="s3")
def s3_fixture(s3_session_fixture: S3Fixture) -> S3Fixture:
    """S3 object storage fixture that reuses the same testcontainer.

    Empties all buckets in case something was left in from a previous test case.
    """
    # we cannot make this an async fixture because it should have function scope
    loop = asyncio.get_event_loop()
    loop.run_until_complete(s3_session_fixture.storage.delete_created_buckets())
    return s3_session_fixture
