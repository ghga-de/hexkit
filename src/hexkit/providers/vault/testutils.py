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
#

"""Utilities for testing code that uses Vault providers.

Please note, only use for testing purposes.
"""

import time
from collections.abc import Generator
from contextlib import suppress
from dataclasses import dataclass

import hvac
import hvac.exceptions
import pytest
from pydantic import SecretStr
from testcontainers.core.container import DockerContainer

from hexkit.custom_types import PytestScope
from hexkit.providers.vault.config import VaultConfig

__all__ = [
    "VAULT_IMAGE",
    "VAULT_TEST_ROOT_TOKEN",
    "VaultFixture",
    "clean_vault_fixture",
    "get_clean_vault_fixture",
    "get_persistent_vault_fixture",
    "get_vault_container_fixture",
    "get_vault_fixture",
    "persistent_vault_fixture",
    "vault_container_fixture",
    "vault_fixture",
]

VAULT_IMAGE = "hashicorp/vault:1.18"
VAULT_TEST_ROOT_TOKEN = "test-root-token"  # noqa: S105


@dataclass(frozen=True)
class VaultFixture:
    """A fixture with utility methods for tests that use Vault."""

    config: VaultConfig
    container: DockerContainer

    def clear_kv(self) -> None:
        """Delete all secrets and metadata under the configured path prefix."""
        client = hvac.Client(
            url=self.config.vault_url,
            token=VAULT_TEST_ROOT_TOKEN,
            verify=self.config.vault_verify,
        )
        mount_point = self.config.vault_secrets_mount_point
        path_prefix = self.config.vault_path.strip()

        def _delete_recursive(path: str) -> None:
            try:
                listing = client.secrets.kv.v2.list_secrets(
                    path=path,
                    mount_point=mount_point,
                )
            except hvac.exceptions.InvalidPath:
                return

            for key in listing["data"]["keys"]:
                if key.endswith("/"):
                    next_path = f"{path}/{key[:-1]}" if path else key[:-1]
                    _delete_recursive(next_path)
                else:
                    full_path = f"{path}/{key}" if path else key
                    with suppress(hvac.exceptions.InvalidPath):
                        client.secrets.kv.v2.delete_metadata_and_all_versions(
                            path=full_path,
                            mount_point=mount_point,
                        )

        _delete_recursive(path_prefix)
        with suppress(hvac.exceptions.InvalidPath):
            client.secrets.kv.v2.delete_metadata_and_all_versions(
                path=path_prefix,
                mount_point=mount_point,
            )


def _vault_container_fixture() -> Generator[VaultFixture, None, None]:
    """Fixture function for getting a running Vault test container."""
    # Create Vault container in dev mode
    container = DockerContainer(image=VAULT_IMAGE)
    container.with_env("VAULT_DEV_ROOT_TOKEN_ID", VAULT_TEST_ROOT_TOKEN)
    container.with_env("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
    container.with_exposed_ports(8200)
    container.with_command("server -dev")

    with container:
        # Get connection details
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8200)
        vault_url = f"http://{host}:{port}"

        # Wait for Vault to be ready (simple retry loop)
        client = hvac.Client(url=vault_url, token=VAULT_TEST_ROOT_TOKEN, verify=False)
        for _ in range(30):
            with suppress(Exception):
                if client.sys.is_initialized():
                    break
            time.sleep(1)
        else:
            raise RuntimeError("Vault did not become ready in time")

        # Enable AppRole auth method
        with suppress(hvac.exceptions.InvalidRequest):
            # Already enabled if exception raised
            client.sys.enable_auth_method(method_type="approle", path="approle")

        # Create a policy that allows full access to the secret/ path
        policy = """
        path "secret/*" {
            capabilities = ["create", "read", "update", "delete", "list"]
        }
        path "secret/data/*" {
            capabilities = ["create", "read", "update", "delete", "list"]
        }
        path "secret/metadata/*" {
            capabilities = ["read", "list", "delete"]
        }
        """
        client.sys.create_or_update_policy(
            name="test-policy",
            policy=policy,
        )

        # Create an AppRole role with the test policy
        client.auth.approle.create_or_update_approle(
            role_name="test-role",
            token_policies=["test-policy"],
        )

        # Get role_id and secret_id
        role_id = client.auth.approle.read_role_id(role_name="test-role")["data"][
            "role_id"
        ]
        secret_id_response = client.auth.approle.generate_secret_id(
            role_name="test-role"
        )
        secret_id = secret_id_response["data"]["secret_id"]

        # Build VaultConfig
        config = VaultConfig(
            vault_url=vault_url,
            vault_secrets_mount_point="secret",
            vault_path="test",
            vault_role_id=SecretStr(role_id),
            vault_secret_id=SecretStr(secret_id),
            vault_verify=False,
        )

        yield VaultFixture(config=config, container=container)


def get_vault_container_fixture(
    scope: PytestScope = "session", name: str = "vault_container"
):
    """Get a Vault test container fixture with desired scope and name.

    By default, the session scope is used for Vault test containers.
    """
    return pytest.fixture(_vault_container_fixture, scope=scope, name=name)


vault_container_fixture = get_vault_container_fixture()


def _vault_fixture(
    vault_container: VaultFixture,
) -> Generator[VaultFixture, None, None]:
    """Fixture function that gets a Vault fixture.

    The state of Vault is not cleaned up by the function.
    """
    yield vault_container


def get_vault_fixture(scope: PytestScope = "function", name: str = "vault"):
    """Get a Vault fixture with desired scope and name.

    The state of the Vault test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Vault test container.
    """
    return pytest.fixture(_vault_fixture, scope=scope, name=name)


def _persistent_vault_fixture(
    vault_container: VaultFixture,
) -> Generator[VaultFixture, None, None]:
    """Fixture function that gets a persistent Vault fixture.

    The state of Vault is not cleaned up by the function.
    """
    yield vault_container


def get_persistent_vault_fixture(scope: PytestScope = "function", name: str = "vault"):
    """Get a Vault fixture with desired scope and name.

    The state of the Vault test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Vault test container.
    """
    return pytest.fixture(_persistent_vault_fixture, scope=scope, name=name)


persistent_vault_fixture = get_persistent_vault_fixture()


def _clean_vault_fixture(
    vault_container: VaultFixture,
) -> Generator[VaultFixture, None, None]:
    """Fixture function that gets a clean Vault fixture.

    The clean state is achieved by deleting all secrets under the
    configured path prefix upfront.
    """
    for vault_fixture in _persistent_vault_fixture(vault_container):
        vault_fixture.clear_kv()
        yield vault_fixture


def get_clean_vault_fixture(scope: PytestScope = "function", name: str = "vault"):
    """Get a Vault fixture with desired scope and name.

    The state of Vault is reset by clearing the configured path prefix
    before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Vault test container.
    """
    return pytest.fixture(_clean_vault_fixture, scope=scope, name=name)


vault_fixture = clean_vault_fixture = get_clean_vault_fixture()
