# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Test the S3-based KVStore providers."""

import asyncio
import json
import re
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest
import pytest_asyncio

from hexkit.custom_types import JsonObject
from hexkit.providers.s3.provider.kvstore import (
    S3BytesKeyValueStore,
    S3DtoKeyValueStore,
    S3JsonKeyValueStore,
    S3StrKeyValueStore,
)
from hexkit.providers.s3.testutils import (
    S3Fixture,
    s3_container_fixture,  # noqa: F401
    s3_fixture,  # noqa: F401
)
from tests.integration.providers.kvstore.kvstore_test_suites import (
    BytesKVStoreTestSuite,
    DtoKVStoreTestSuite,
    JsonKVStoreTestSuite,
    SimpleDto,
    StrKVStoreTestSuite,
)

pytestmark = pytest.mark.asyncio()

KV_BUCKET = "kvstore"
CUSTOM_BUCKET = "custom-bucket"


# Fixtures for each store type


@pytest_asyncio.fixture()
async def json_kvstore(
    s3: S3Fixture,
) -> AsyncGenerator[S3JsonKeyValueStore, None]:
    """Provide a JSON KVStore backed by a clean S3 bucket."""
    await s3.storage.create_bucket(KV_BUCKET)
    yield S3JsonKeyValueStore(config=s3.config, bucket_id=KV_BUCKET)


@pytest_asyncio.fixture()
async def str_kvstore(
    s3: S3Fixture,
) -> AsyncGenerator[S3StrKeyValueStore, None]:
    """Provide a string KVStore backed by a clean S3 bucket."""
    await s3.storage.create_bucket(KV_BUCKET)
    yield S3StrKeyValueStore(config=s3.config, bucket_id=KV_BUCKET)


@pytest_asyncio.fixture()
async def bytes_kvstore(
    s3: S3Fixture,
) -> AsyncGenerator[S3BytesKeyValueStore, None]:
    """Provide a bytes KVStore backed by a clean S3 bucket."""
    await s3.storage.create_bucket(KV_BUCKET)
    yield S3BytesKeyValueStore(config=s3.config, bucket_id=KV_BUCKET)


@pytest_asyncio.fixture()
async def dto_kvstore(
    s3: S3Fixture,
) -> AsyncGenerator[S3DtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO KVStore backed by a clean S3 bucket."""
    await s3.storage.create_bucket(KV_BUCKET)
    yield S3DtoKeyValueStore(config=s3.config, dto_model=SimpleDto, bucket_id=KV_BUCKET)


@pytest.fixture()
def dto_store_factory(
    s3: S3Fixture,
) -> Callable[..., AbstractAsyncContextManager[S3DtoKeyValueStore[Any]]]:
    """Factory to construct DTO KVStore for arbitrary models and bucket IDs."""

    def factory(dto_model, **kwargs):
        bucket_id = kwargs.get("bucket_id", KV_BUCKET)

        @asynccontextmanager
        async def _ctx():
            if not await s3.storage.does_bucket_exist(bucket_id):
                await s3.storage.create_bucket(bucket_id)
            yield S3DtoKeyValueStore(config=s3.config, dto_model=dto_model, **kwargs)

        return _ctx()

    return factory


@pytest.fixture()
def kvstore_custom_kwargs() -> dict[str, Any]:
    """Provider-specific kwargs for custom options tests: a non-default bucket."""
    return {"bucket_id": CUSTOM_BUCKET}


# Common tests using shared test suites


class TestS3JsonKVStore(JsonKVStoreTestSuite):
    """Tests for S3 JSON KVStore implementation."""


class TestS3StrKVStore(StrKVStoreTestSuite):
    """Tests for S3 String KVStore implementation."""


class TestS3BytesKVStore(BytesKVStoreTestSuite):
    """Tests for S3 Bytes KVStore implementation."""


class TestS3DtoKVStore(DtoKVStoreTestSuite):
    """Tests for S3 DTO KVStore implementation."""


# S3-specific tests


async def test_store_repr(s3: S3Fixture):
    """Test the repr of the string value store."""
    custom_bucket = "custom-test-bucket"
    await s3.storage.create_bucket(custom_bucket)
    store = S3StrKeyValueStore(config=s3.config, bucket_id=custom_bucket)
    store_repr = repr(store)
    store_repr = re.sub(r"SecretStr\('[^']*'\)", "<secret>", store_repr)

    assert store_repr.startswith("S3StrKeyValueStore(config=S3Config(")
    assert f"s3_access_key_id={s3.config.s3_access_key_id!r}" in store_repr
    assert "s3_secret_access_key=<secret>" in store_repr
    assert f"bucket_id={custom_bucket!r}" in store_repr


# Whitebox/internals tests


async def test_str_whitebox_storage_format(s3: S3Fixture):
    """Verify string values are stored as raw UTF-8 bytes in S3."""
    await s3.storage.create_bucket(KV_BUCKET)
    store = S3StrKeyValueStore(config=s3.config, bucket_id=KV_BUCKET)

    value = "hello, world!"
    await store.set("str-key", value)

    response = await asyncio.to_thread(
        store._client.get_object, Bucket=KV_BUCKET, Key="str-key"
    )
    raw = await asyncio.to_thread(response["Body"].read)
    assert raw == value.encode("utf-8")


async def test_json_whitebox_storage_format(s3: S3Fixture):
    """Verify JSON values are stored as UTF-8-encoded JSON bytes in S3."""
    await s3.storage.create_bucket(KV_BUCKET)
    store = S3JsonKeyValueStore(config=s3.config, bucket_id=KV_BUCKET)
    payload: JsonObject = {"foo": True, "bar": [1, 2, 3]}
    await store.set("json-key", payload)

    response = await asyncio.to_thread(
        store._client.get_object, Bucket=KV_BUCKET, Key="json-key"
    )
    raw = await asyncio.to_thread(response["Body"].read)
    assert json.loads(raw.decode("utf-8")) == payload


async def test_dto_whitebox_storage_format(s3: S3Fixture):
    """Verify DTOs are stored as JSON bytes using model_dump_json in S3."""
    await s3.storage.create_bucket(KV_BUCKET)
    store = S3DtoKeyValueStore(
        config=s3.config, dto_model=SimpleDto, bucket_id=KV_BUCKET
    )
    dto = SimpleDto(name="test", value=42)
    await store.set("dto-key", dto)

    response = await asyncio.to_thread(
        store._client.get_object, Bucket=KV_BUCKET, Key="dto-key"
    )
    raw = await asyncio.to_thread(response["Body"].read)
    assert json.loads(raw.decode("utf-8")) == dto.model_dump()


# Bucket isolation tests


async def test_bucket_isolation(s3: S3Fixture):
    """Test that different bucket_ids keep their data completely isolated."""
    bucket1 = "bucket-one"
    bucket2 = "bucket-two"
    await s3.storage.create_bucket(bucket1)
    await s3.storage.create_bucket(bucket2)

    store1 = S3JsonKeyValueStore(config=s3.config, bucket_id=bucket1)
    store2 = S3JsonKeyValueStore(config=s3.config, bucket_id=bucket2)

    await store1.set("shared", {"from": 1})
    await store2.set("shared", {"from": 2})

    assert await store1.get("shared") == {"from": 1}
    assert await store2.get("shared") == {"from": 2}

    await store1.delete("shared")
    assert await store1.get("shared") is None
    assert await store2.get("shared") == {"from": 2}
