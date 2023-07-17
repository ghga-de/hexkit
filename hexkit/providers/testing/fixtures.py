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
"""Enable requesting test fixtures with chosen scope"""

from typing import Callable, Type, Union

import pytest_asyncio
from pytest_asyncio.plugin import _ScopeName

from hexkit.providers.akafka.testutils import KafkaFixture, kafka_fixture_function
from hexkit.providers.mongodb.testutils import MongoDbFixture, mongodb_fixture_function
from hexkit.providers.s3.testutils import S3Fixture, s3_fixture_function

__all__ = [
    "get_fixture",
    "mongodb_fixture",
    "kafka_fixture",
    "s3_fixture",
]


ProviderFixture = Union[Type[KafkaFixture], Type[MongoDbFixture], Type[S3Fixture]]

fixture_type_to_function: dict[ProviderFixture, Callable] = {
    KafkaFixture: kafka_fixture_function,
    MongoDbFixture: mongodb_fixture_function,
    S3Fixture: s3_fixture_function,
}


def get_fixture(fixture_type: ProviderFixture, scope: _ScopeName = "function"):
    """Produce a test fixture with the desired scope"""
    fixture_function = fixture_type_to_function[fixture_type]
    return pytest_asyncio.fixture(scope=scope)(fixture_function)


mongodb_fixture = get_fixture(MongoDbFixture)
kafka_fixture = get_fixture(KafkaFixture)
s3_fixture = get_fixture(S3Fixture)
