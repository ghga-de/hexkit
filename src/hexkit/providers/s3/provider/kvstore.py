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

"""S3-based provider implementing the KeyValueStoreProtocol."""

import asyncio
import json
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Any, Generic

import boto3
import botocore.exceptions

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import Dto
from hexkit.protocols.kvstore import KeyValueStoreProtocol
from hexkit.providers.s3.config import S3Config
from hexkit.providers.s3.provider.objstorage import read_aws_config_ini

__all__ = [
    "S3BytesKeyValueStore",
    "S3DtoKeyValueStore",
    "S3JsonKeyValueStore",
    "S3StrKeyValueStore",
]


DEFAULT_KV_BUCKET_ID = "kvstore"


class S3BaseKeyValueStore(ABC, KeyValueStoreProtocol):
    """Base class for S3 key-value stores providing common functionality.

    Each key is stored as an S3 object within the configured bucket. The bucket
    must already exist before using the store.
    """

    def __init__(
        self,
        *,
        config: S3Config,
        bucket_id: str = DEFAULT_KV_BUCKET_ID,
        **kwargs,
    ):
        """Initialize the provider with configuration and bucket name.

        Args:
            config: S3-specific config parameters.
            bucket_id: Name of the existing S3 bucket to hold the key-value pairs.
                Defaults to 'kvstore'.

        You might be able to pass additional arguments specific to subclasses.
        """
        config_args = ", ".join(
            f"{k}={v!r}" for k, v in config.model_dump(exclude_defaults=True).items()
        )
        args = [f"config=S3Config({config_args})"]
        if bucket_id != DEFAULT_KV_BUCKET_ID:
            args.append(f"bucket_id={bucket_id!r}")
        for key, val in kwargs.items():
            with suppress(AttributeError):
                val = val.__name__  # short repr e.g. for model classes
            args.append(f"{key}={val}")
        self._repr = f"{self.__class__.__name__}({' '.join(args)})"

        self._bucket_id = bucket_id

        advanced_config = (
            None
            if config.aws_config_ini is None
            else read_aws_config_ini(config.aws_config_ini)
        )
        session_token = (
            None
            if config.s3_session_token is None
            else config.s3_session_token.get_secret_value()
        )
        self._client = boto3.client(
            service_name="s3",
            endpoint_url=config.s3_endpoint_url,
            aws_access_key_id=config.s3_access_key_id,
            aws_secret_access_key=config.s3_secret_access_key.get_secret_value(),
            aws_session_token=session_token,
            config=advanced_config,
        )

    def __repr__(self) -> str:
        return self._repr

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        # S3 DeleteObject is a no-op for non-existent keys
        await asyncio.to_thread(
            self._client.delete_object,
            Bucket=self._bucket_id,
            Key=key,
        )

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        try:
            await asyncio.to_thread(
                self._client.head_object,
                Bucket=self._bucket_id,
                Key=key,
            )
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
        return True

    @abstractmethod
    async def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes before writing it to S3."""

    @abstractmethod
    async def _decode_value(self, raw: bytes) -> Any:
        """Decode bytes read from S3 back to a value."""

    async def get(self, key: str, default=None):
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        try:
            response = await asyncio.to_thread(
                self._client.get_object,
                Bucket=self._bucket_id,
                Key=key,
            )
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return default
            raise
        raw = await asyncio.to_thread(response["Body"].read)
        return await self._decode_value(raw)

    async def set(self, key: str, value) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        raw = await self._encode_value(value)
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket_id,
            Key=key,
            Body=raw,
        )


class S3JsonKeyValueStore(S3BaseKeyValueStore):
    """S3-specific KVStore provider for JSON data."""

    async def _encode_value(self, value: JsonObject) -> bytes:
        """Encode a JSON object to UTF-8 bytes."""
        if not isinstance(value, dict):
            raise TypeError("Value must be a dict representing a JSON object")
        return json.dumps(value).encode()

    async def _decode_value(self, raw: bytes) -> JsonObject:
        """Decode UTF-8 bytes to a JSON object."""
        return json.loads(raw.decode())


class S3StrKeyValueStore(S3BaseKeyValueStore):
    """S3-specific KVStore provider for string data."""

    async def _encode_value(self, value: str) -> bytes:
        """Encode a string to UTF-8 bytes."""
        if not isinstance(value, str):
            raise TypeError("Value must be of type str")
        return value.encode()

    async def _decode_value(self, raw: bytes) -> str:
        """Decode UTF-8 bytes to a string."""
        return raw.decode()


class S3BytesKeyValueStore(S3BaseKeyValueStore):
    """S3-specific KVStore provider for binary (bytes) data."""

    async def _encode_value(self, value: bytes) -> bytes:
        """Bytes values are stored as-is."""
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        return value

    async def _decode_value(self, raw: bytes) -> bytes:
        """Bytes values are retrieved as-is."""
        return raw


class S3DtoKeyValueStore(S3BaseKeyValueStore, Generic[Dto]):
    """S3-specific KVStore provider for Pydantic model (DTO) data.

    This store allows arbitrary DTOs (Pydantic models) to be stored as values,
    with the DTO model passed in the constructor.
    """

    _dto_model: type[Dto]

    def __init__(
        self,
        *,
        config: S3Config,
        dto_model: type[Dto],
        bucket_id: str = DEFAULT_KV_BUCKET_ID,
    ):
        """Initialize the provider with configuration, model type, and bucket name.

        Args:
            config: S3-specific config parameters.
            dto_model: The Pydantic model type to support for values.
            bucket_id: Name of the existing S3 bucket to hold the key-value pairs.
                Defaults to 'kvstore'.
        """
        super().__init__(
            config=config,
            bucket_id=bucket_id,
            dto_model=dto_model,
        )
        self._dto_model = dto_model

    async def _encode_value(self, value: Dto) -> bytes:
        """Serialize DTO to JSON bytes for storage."""
        if not isinstance(value, self._dto_model):
            raise TypeError(f"Value must be of type {self._dto_model.__name__}")
        return value.model_dump_json().encode()

    async def _decode_value(self, raw: bytes) -> Dto:
        """Deserialize JSON bytes from storage back to DTO."""
        return self._dto_model.model_validate_json(raw)
