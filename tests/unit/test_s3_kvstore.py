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

"""Unit tests for the S3-based KeyValueStore providers."""

import json
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from pydantic import BaseModel, SecretStr

from hexkit.providers.s3 import S3Config
from hexkit.providers.s3.provider.kvstore import (
    S3BytesKeyValueStore,
    S3DtoKeyValueStore,
    S3JsonKeyValueStore,
    S3StrKeyValueStore,
)

pytestmark = pytest.mark.asyncio()

CONFIG = S3Config(  # type: ignore [call-arg]
    s3_endpoint_url="http://localhost:9000",
    s3_access_key_id="test-key-id",
    s3_secret_access_key=SecretStr("test-secret"),
)

BUCKET = "test-bucket"
KEY = "some/key"


def make_not_found_error() -> botocore.exceptions.ClientError:
    """Create a ClientError simulating a missing key."""
    return botocore.exceptions.ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        },
        "GetObject",
    )


def make_store_with_mock(store_cls, **kwargs):
    """Construct a store instance, replacing the boto3 client with a MagicMock."""
    with patch("hexkit.providers.s3.provider.kvstore.boto3.client") as mock_client_fn:
        mock_client = MagicMock()
        mock_client_fn.return_value = mock_client
        store = store_cls(config=CONFIG, bucket_id=BUCKET, **kwargs)
    return store, mock_client


# ---------------------------------------------------------------------------
# S3BytesKeyValueStore
# ---------------------------------------------------------------------------


async def test_bytes_set_and_get():
    """set() encodes bytes correctly; get() decodes them back."""
    store, client = make_store_with_mock(S3BytesKeyValueStore)
    value = b"hello bytes"
    body_mock = MagicMock()
    body_mock.read.return_value = value
    client.get_object.return_value = {"Body": body_mock}

    await store.set(KEY, value)
    client.put_object.assert_called_once_with(Bucket=BUCKET, Key=KEY, Body=value)

    result = await store.get(KEY)
    client.get_object.assert_called_once_with(Bucket=BUCKET, Key=KEY)
    assert result == value


async def test_bytes_get_missing_returns_default():
    """get() returns the default when the key does not exist."""
    store, client = make_store_with_mock(S3BytesKeyValueStore)
    client.get_object.side_effect = make_not_found_error()

    result = await store.get(KEY, default=b"fallback")
    assert result == b"fallback"


async def test_bytes_delete():
    """delete() calls delete_object on the correct bucket and key."""
    store, client = make_store_with_mock(S3BytesKeyValueStore)

    await store.delete(KEY)
    client.delete_object.assert_called_once_with(Bucket=BUCKET, Key=KEY)


async def test_bytes_exists_true():
    """exists() returns True when head_object succeeds."""
    store, client = make_store_with_mock(S3BytesKeyValueStore)
    client.head_object.return_value = {}

    assert await store.exists(KEY) is True
    client.head_object.assert_called_once_with(Bucket=BUCKET, Key=KEY)


async def test_bytes_exists_false():
    """exists() returns False when head_object raises a 404."""
    store, client = make_store_with_mock(S3BytesKeyValueStore)
    client.head_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )

    assert await store.exists(KEY) is False


async def test_bytes_type_error():
    """set() raises TypeError for non-bytes values."""
    store, _ = make_store_with_mock(S3BytesKeyValueStore)
    with pytest.raises(TypeError):
        await store.set(KEY, "not bytes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# S3StrKeyValueStore
# ---------------------------------------------------------------------------


async def test_str_set_and_get():
    """set() encodes string as UTF-8; get() decodes it back."""
    store, client = make_store_with_mock(S3StrKeyValueStore)
    value = "hello string"
    encoded = value.encode()
    body_mock = MagicMock()
    body_mock.read.return_value = encoded
    client.get_object.return_value = {"Body": body_mock}

    await store.set(KEY, value)
    client.put_object.assert_called_once_with(Bucket=BUCKET, Key=KEY, Body=encoded)

    result = await store.get(KEY)
    assert result == value


async def test_str_get_missing_returns_none():
    """get() returns None by default when key is absent."""
    store, client = make_store_with_mock(S3StrKeyValueStore)
    client.get_object.side_effect = make_not_found_error()

    assert await store.get(KEY) is None


async def test_str_type_error():
    """set() raises TypeError for non-string values."""
    store, _ = make_store_with_mock(S3StrKeyValueStore)
    with pytest.raises(TypeError):
        await store.set(KEY, 42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# S3JsonKeyValueStore
# ---------------------------------------------------------------------------


async def test_json_set_and_get():
    """set() serialises dict to JSON bytes; get() parses it back."""
    store, client = make_store_with_mock(S3JsonKeyValueStore)
    value = {"answer": 42, "nested": {"ok": True}}
    encoded = json.dumps(value).encode()
    body_mock = MagicMock()
    body_mock.read.return_value = encoded
    client.get_object.return_value = {"Body": body_mock}

    await store.set(KEY, value)
    client.put_object.assert_called_once_with(Bucket=BUCKET, Key=KEY, Body=encoded)

    result = await store.get(KEY)
    assert result == value


async def test_json_type_error():
    """set() raises TypeError when value is not a dict."""
    store, _ = make_store_with_mock(S3JsonKeyValueStore)
    with pytest.raises(TypeError):
        await store.set(KEY, "not a dict")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# S3DtoKeyValueStore
# ---------------------------------------------------------------------------


class ItemModel(BaseModel):
    """Simple Pydantic model for testing the DTO store."""

    name: str
    count: int


async def test_dto_set_and_get():
    """set() serialises a DTO to JSON bytes; get() validates it back to the model."""
    store, client = make_store_with_mock(S3DtoKeyValueStore, dto_model=ItemModel)
    value = ItemModel(name="widget", count=7)
    encoded = value.model_dump_json().encode()
    body_mock = MagicMock()
    body_mock.read.return_value = encoded
    client.get_object.return_value = {"Body": body_mock}

    await store.set(KEY, value)
    client.put_object.assert_called_once_with(Bucket=BUCKET, Key=KEY, Body=encoded)

    result = await store.get(KEY)
    assert isinstance(result, ItemModel)
    assert result.name == "widget"
    assert result.count == 7


async def test_dto_get_missing_returns_default():
    """get() returns the provided default when the key is absent."""
    store, client = make_store_with_mock(S3DtoKeyValueStore, dto_model=ItemModel)
    client.get_object.side_effect = make_not_found_error()
    default = ItemModel(name="fallback", count=0)

    result = await store.get(KEY, default=default)
    assert result == default


async def test_dto_type_error():
    """set() raises TypeError when value is not an instance of the configured model."""
    store, _ = make_store_with_mock(S3DtoKeyValueStore, dto_model=ItemModel)
    with pytest.raises(TypeError):
        await store.set(KEY, {"name": "widget", "count": 1})  # plain dict, not model


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


async def test_repr_default_bucket():
    """__repr__ works and contains the class name."""
    store, _ = make_store_with_mock(S3StrKeyValueStore)
    assert "S3StrKeyValueStore" in repr(store)


async def test_repr_custom_bucket():
    """__repr__ includes the custom bucket_id when it differs from the default."""
    store, _ = make_store_with_mock(S3StrKeyValueStore)
    # We used BUCKET = "test-bucket" which differs from the default "kvstore"
    assert "test-bucket" in repr(store)
