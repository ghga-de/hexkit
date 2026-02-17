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

"""Vault-based provider implementing the KVStoreProtocol."""

import asyncio
import base64
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from typing import Any, Generic

import hvac.exceptions

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import Dto
from hexkit.protocols.kvstore import KeyValueStoreProtocol
from hexkit.providers.vault.config import VaultConfig
from hexkit.providers.vault.provider.client import ConfiguredVaultClient

__all__ = [
    "VaultBytesKeyValueStore",
    "VaultDtoKeyValueStore",
    "VaultJsonKeyValueStore",
    "VaultStrKeyValueStore",
]

DEFAULT_PATH_PREFIX = ""


class VaultBaseKeyValueStore(ABC, KeyValueStoreProtocol):
    """Base class for Vault key-value stores providing common functionality.

    Vault KV v2 secrets engine stores values as strings or dicts,
    so this base class handles the common Vault operations and
    subclasses provide encoding/decoding for their specific value types.
    """

    _repr: str
    _client: ConfiguredVaultClient
    _config: VaultConfig

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: VaultConfig,
        **kwargs,
    ):
        """Yields a store instance with the provided configuration.

        The Store will use the vault_path from config for all paths.
        The client connection is established and closed automatically.

        Args:
            config: Vault-specific config parameters.

        You might be able to pass additional arguments specific to subclasses.
        """
        async with ConfiguredVaultClient(config) as client:
            yield cls(
                client=client,
                config=config,
                **kwargs,
            )

    def __init__(
        self,
        *,
        client: ConfiguredVaultClient,
        config: VaultConfig,
        **kwargs,
    ):
        """Initialize the provider with configuration.

        Args:
            client: An instance of a configured Vault client.
            config: Vault-specific config parameters.

        You might be able to pass additional arguments specific to subclasses.
        """
        config_args = ", ".join(
            f"{k}={v!r}" for k, v in config.model_dump(exclude_defaults=True).items()
        )
        args = [f"config=VaultConfig({config_args})"]
        for key, val in kwargs.items():
            with suppress(AttributeError):
                val = val.__name__  # short repr e.g. for model classes
            args.append(f"{key}={val}")
        self._repr = f"{self.__class__.__name__}({', '.join(args)})"
        self._check_auth = client._check_auth
        self._api = client._client.secrets.kv.v2
        self._path = config.vault_path.strip("/")
        self._mount_point = config.vault_secrets_mount_point

    def __repr__(self) -> str:
        return self._repr

    def _make_path(self, key: str) -> str:
        """Construct the full Vault path using vault_path from config."""
        path = self._path
        return f"{path}/{key}" if path else key

    @abstractmethod
    def _encode_value(self, value: Any) -> Any:
        """Encode a value before writing it to Vault."""
        pass

    @abstractmethod
    def _decode_value(self, data: Any) -> Any:
        """Decode data read from Vault to the target type."""
        pass

    async def get(self, key: str, default=None):
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        path = self._make_path(key)
        await self._check_auth()
        try:
            response = await asyncio.to_thread(
                self._api.read_secret_version,
                path=path,
                mount_point=self._mount_point,
                raise_on_deleted_version=True,
            )
            data = response["data"]["data"]["value"]
            return self._decode_value(data)
        except hvac.exceptions.InvalidPath:
            return default

    async def set(self, key: str, value) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        path = self._make_path(key)
        await self._check_auth()
        encoded = self._encode_value(value)
        await asyncio.to_thread(
            self._api.create_or_update_secret,
            path=path,
            secret={"value": encoded},
            mount_point=self._mount_point,
        )

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        path = self._make_path(key)
        await self._check_auth()
        with suppress(hvac.exceptions.InvalidPath):
            await asyncio.to_thread(
                self._api.delete_metadata_and_all_versions,
                path=path,
                mount_point=self._mount_point,
            )

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        result = await self.get(key)
        return result is not None


class VaultBytesKeyValueStore(VaultBaseKeyValueStore):
    """Vault-specific KVStore provider for binary (bytes) data.

    Bytes are base64-encoded before storage and decoded on retrieval.
    """

    def _encode_value(self, value: bytes) -> str:
        """Encode bytes to base64 string."""
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        return base64.b64encode(value).decode("utf-8")

    def _decode_value(self, data: str) -> bytes:
        """Decode base64 string to bytes."""
        return base64.b64decode(data)


class VaultStrKeyValueStore(VaultBaseKeyValueStore):
    """Vault-specific KVStore provider for string data.

    Strings are stored directly as Vault supports string values natively.
    """

    def _encode_value(self, value: str) -> str:
        """Strings are stored as-is."""
        if not isinstance(value, str):
            raise TypeError("Value must be of type str")
        return value

    def _decode_value(self, data: str) -> str:
        """Strings are retrieved as-is."""
        return data


class VaultJsonKeyValueStore(VaultBaseKeyValueStore):
    """Vault-specific KVStore provider for JSON data.

    JSON objects are stored directly as dicts since Vault KV v2 supports
    structured data natively.
    """

    def _encode_value(self, value: JsonObject) -> dict:
        """JSON objects are stored as-is."""
        if not isinstance(value, dict):
            raise TypeError("Value must be a dict representing a JSON object")
        return value

    def _decode_value(self, data: dict) -> JsonObject:
        """JSON objects are retrieved as-is."""
        return data


class VaultDtoKeyValueStore(VaultBaseKeyValueStore, Generic[Dto]):
    """Vault-specific KVStore provider for Pydantic model (DTO) data.

    This store allows arbitrary DTOs (Pydantic models) to be stored as values,
    with the DTO model passed in the constructor. DTOs are serialized using
    Pydantic's model_dump and stored as dicts in Vault.
    """

    _dto_model: type[Dto]

    def __init__(
        self,
        *,
        client: ConfiguredVaultClient,
        config: VaultConfig,
        dto_model: type[Dto],
        **kwargs,
    ):
        """Initialize the provider with configuration and model type.

        Args:
            client: An instance of a configured Vault client.
            config: Vault-specific config parameters.
            dto_model: The Pydantic model type to support for values.

        You might be able to pass additional arguments specific to subclasses.
        """
        super().__init__(
            client=client,
            config=config,
            dto_model=dto_model,
            **kwargs,
        )
        self._dto_model = dto_model

    def _encode_value(self, value: Dto) -> dict:
        """Transform DTO to dict using Pydantic's model_dump."""
        if not isinstance(value, self._dto_model):
            raise TypeError(f"Value must be of type {self._dto_model.__name__}")
        return value.model_dump(mode="json")

    def _decode_value(self, data: dict) -> Dto:
        """Parse dict and validate as DTO."""
        return self._dto_model.model_validate(data)
