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

from hexkit.providers.akafka.testutils import (
    EventBase,
    EventRecorder,
    ExpectedEvent,
    KafkaFixture,
    RecordedEvent,
    ValidationError,
    kafka_fixture_function,
)
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    config_from_mongodb_container,
    mongodb_fixture_function,
)
from hexkit.providers.s3.testutils import (
    MEBIBYTE,
    TEST_FILE_DIR,
    TEST_FILE_PATHS,
    TIMEOUT,
    FileObject,
    S3Config,
    S3Fixture,
    S3ObjectStorage,
    calc_md5,
    check_part_size,
    config_from_localstack_container,
    download_and_check_test_file,
    file_fixture,
    get_initialized_upload,
    multipart_upload_file,
    populate_storage,
    prepare_non_completed_upload,
    s3_fixture_function,
    temp_file_object,
    typical_workflow,
    upload_file,
    upload_part,
    upload_part_of_size,
    upload_part_via_url,
)

__all__ = [
    "EventBase",
    "EventRecorder",
    "ExpectedEvent",
    "KafkaFixture",
    "RecordedEvent",
    "ValidationError",
    "kafka_fixture_function",
    "MongoDbFixture",
    "config_from_mongodb_container",
    "mongodb_fixture_function",
    "MEBIBYTE",
    "TEST_FILE_DIR",
    "TEST_FILE_PATHS",
    "TIMEOUT",
    "FileObject",
    "S3Config",
    "S3Fixture",
    "S3ObjectStorage",
    "calc_md5",
    "check_part_size",
    "config_from_localstack_container",
    "download_and_check_test_file",
    "file_fixture",
    "get_initialized_upload",
    "multipart_upload_file",
    "populate_storage",
    "prepare_non_completed_upload",
    "s3_fixture_function",
    "temp_file_object",
    "typical_workflow",
    "upload_file",
    "upload_part",
    "upload_part_of_size",
    "upload_part_via_url",
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
