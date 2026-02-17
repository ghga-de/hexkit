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

"""Vault client configuration and connection management."""

import asyncio
from pathlib import Path
from typing import Any

import hvac

from hexkit.providers.vault.config import VaultConfig

__all__ = ["ConfiguredVaultClient"]


class ConfiguredVaultClient:
    """Context manager for Vault client with automatic connection lifecycle.

    Supports AppRole and Kubernetes authentication methods.
    Uses Vault KV v2 API exclusively.
    """

    def __init__(self, config: VaultConfig):
        """Initialize with Vault configuration.

        Args:
            config: Vault-specific config parameters.
        """
        self._config = config
        self._client = hvac.Client(url=config.vault_url, verify=config.vault_verify)

    async def __aenter__(self) -> "ConfiguredVaultClient":
        """Establish Vault connection and authenticate."""
        await self._login()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Close Vault connection."""
        # hvac client doesn't require explicit cleanup
        pass

    async def _login(self) -> None:
        """Authenticate with Vault using configured method.

        Raises:
            ValueError: If no authentication method is properly configured.
            RuntimeError: If authentication fails.
        """
        # Determine authentication method
        if self._config.vault_kube_role:
            await self._login_kubernetes()
        elif self._config.vault_role_id and self._config.vault_secret_id:
            await self._login_approle()
        else:
            raise ValueError(
                "No authentication method configured. "
                "Provide either vault_kube_role or both vault_role_id and vault_secret_id."
            )

        # Verify authentication succeeded
        is_authenticated = await asyncio.to_thread(self._client.is_authenticated)
        if not is_authenticated:
            raise RuntimeError("Failed to authenticate with Vault")

    async def _login_approle(self) -> None:
        """Authenticate using AppRole method."""
        # These are guaranteed to be set by validation in _login()
        if self._config.vault_role_id is None or self._config.vault_secret_id is None:
            raise ValueError("AppRole credentials not configured")
        role_id = self._config.vault_role_id.get_secret_value()
        secret_id = self._config.vault_secret_id.get_secret_value()
        mount_point = self._config.vault_auth_mount_point or "approle"

        def login():
            self._client.auth.approle.login(
                role_id=role_id, secret_id=secret_id, mount_point=mount_point
            )

        await asyncio.to_thread(login)

    async def _login_kubernetes(self) -> None:
        """Authenticate using Kubernetes method.

        Raises:
            FileNotFoundError: If service account token file doesn't exist.
        """
        token_path = Path(self._config.service_account_token_path)
        if not token_path.exists():
            raise FileNotFoundError(f"Service account token not found at {token_path}")

        jwt = token_path.read_text().strip()
        role = self._config.vault_kube_role
        mount_point = self._config.vault_auth_mount_point or "kubernetes"

        def login():
            self._client.auth.kubernetes.login(
                role=role, jwt=jwt, mount_point=mount_point
            )

        await asyncio.to_thread(login)

    async def _check_auth(self) -> bool:
        """Check if client is authenticated, re-authenticate if needed.

        Returns:
            True if authenticated (after re-auth if necessary).
        """
        is_authenticated = await asyncio.to_thread(self._client.is_authenticated)

        if not is_authenticated:
            await self._login()
            is_authenticated = await asyncio.to_thread(self._client.is_authenticated)

        return is_authenticated
