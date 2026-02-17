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

"""Test the Vault-based KVStore providers."""

import re
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
import pytest_asyncio

from hexkit.providers.vault import (
    VaultBytesKeyValueStore,
    VaultDtoKeyValueStore,
    VaultJsonKeyValueStore,
    VaultStrKeyValueStore,
)
from hexkit.providers.vault.testutils import (
    VaultFixture,
    vault_container_fixture,  # noqa: F401
    vault_fixture,  # noqa: F401
)
from tests.integration.providers.kvstore.kvstore_test_suites import (
    BytesKVStoreTestSuite,
    DtoKVStoreTestSuite,
    JsonKVStoreTestSuite,
    SimpleDto,
    StrKVStoreTestSuite,
)

pytestmark = pytest.mark.asyncio()


# Fixtures for each store type
@pytest_asyncio.fixture()
async def json_kvstore(
    vault: VaultFixture,
) -> AsyncGenerator[VaultJsonKeyValueStore, None]:
    """Provide a JSON KVStore bound to a test Vault instance."""
    async with VaultJsonKeyValueStore.construct(
        config=vault.config, path_prefix="test_json"
    ) as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore(
    vault: VaultFixture,
) -> AsyncGenerator[VaultStrKeyValueStore, None]:
    """Provide a string KVStore bound to a test Vault instance."""
    async with VaultStrKeyValueStore.construct(
        config=vault.config, path_prefix="test_str"
    ) as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore(
    vault: VaultFixture,
) -> AsyncGenerator[VaultBytesKeyValueStore, None]:
    """Provide a bytes KVStore bound to a test Vault instance."""
    async with VaultBytesKeyValueStore.construct(
        config=vault.config, path_prefix="test_bytes"
    ) as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore(
    vault: VaultFixture,
) -> AsyncGenerator[VaultDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO KVStore bound to a test Vault instance."""
    async with VaultDtoKeyValueStore.construct(
        config=vault.config, dto_model=SimpleDto, path_prefix="test_dto"
    ) as store:
        yield store


@pytest.fixture()
def dto_store_factory(
    vault: VaultFixture,
) -> Callable[..., AbstractAsyncContextManager[VaultDtoKeyValueStore[Any]]]:
    """Factory to construct DTO KVStore for arbitrary models."""

    def factory(dto_model, path_prefix: str = "test_factory", **kwargs):
        return VaultDtoKeyValueStore.construct(
            config=vault.config,
            dto_model=dto_model,
            path_prefix=path_prefix,
            **kwargs,
        )

    return factory


@pytest.fixture()
def kvstore_custom_kwargs() -> dict[str, Any]:
    """Provider-specific kwargs for custom options tests."""
    return {"path_prefix": "custom"}


# Common tests using shared test suites


class TestVaultBytesKVStore(BytesKVStoreTestSuite):
    """Tests for Vault Bytes KVStore implementation."""


class TestVaultStrKVStore(StrKVStoreTestSuite):
    """Tests for Vault String KVStore implementation."""


class TestVaultJsonKVStore(JsonKVStoreTestSuite):
    """Tests for Vault JSON KVStore implementation."""


class TestVaultDtoKVStore(DtoKVStoreTestSuite):
    """Tests for Vault DTO KVStore implementation."""


# Vault-specific tests


async def test_store_repr(vault: VaultFixture):
    """Test the repr of the string value store."""
    async with VaultStrKeyValueStore.construct(config=vault.config) as store:
        store_repr = repr(store)
        store_repr = re.sub(r"'http://[^',)]+'", "<url>", store_repr)
        store_repr = re.sub(r"SecretStr\('[^']+'\)", "<secret>", store_repr)

        assert store_repr == (
            "VaultStrKeyValueStore(config=VaultConfig("
            "vault_url=<url>, vault_path='test', "
            "vault_role_id=<secret>, vault_secret_id=<secret>, "
            "vault_verify=False))"
        )
