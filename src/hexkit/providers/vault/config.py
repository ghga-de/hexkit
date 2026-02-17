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

"""HashiCorp Vault-specific configuration."""

from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings

__all__ = ["VaultConfig"]


class VaultConfig(BaseSettings):
    """Configuration parameters for connecting to a HashiCorp Vault server.

    Inherit your config class from this class if your application uses Vault.
    At least one authentication method must be configured (AppRole or Kubernetes).
    """

    vault_url: str = Field(
        ...,
        examples=["http://localhost:8200", "https://vault.example.com:8200"],
        description="URL of the Vault server.",
    )
    vault_path: str = Field(
        ...,
        examples=["myapp/secrets", "production/config"],
        pattern=r"^(?!/).*(?<!/)$",
        description=(
            "Path prefix for all keys stored in Vault."
            + " Should not contain leading or trailing slashes."
        ),
    )
    vault_secrets_mount_point: str = Field(
        default="secret",
        examples=["secret", "kv"],
        description="Mount point for the KV v2 secrets engine in Vault.",
    )
    vault_role_id: SecretStr | None = Field(
        default=None,
        examples=["example_role"],
        description=(
            "AppRole role ID for authentication."
            + " Required when using AppRole authentication."
        ),
    )
    vault_secret_id: SecretStr | None = Field(
        default=None,
        examples=["example_secret"],
        description=(
            "AppRole secret ID for authentication."
            + " Required when using AppRole authentication."
        ),
    )
    vault_kube_role: str | None = Field(
        default=None,
        examples=["myapp-role", "production-reader"],
        description=(
            "Kubernetes authentication role name."
            + " Required when using Kubernetes authentication."
        ),
    )
    vault_verify: bool | str = Field(
        default=True,
        examples=[True, False, "/path/to/ca-bundle.crt"],
        description=(
            "SSL certificate verification for Vault connections."
            + " Can be True (verify using system CA bundle), False (skip verification),"
            + " or a string path to a custom CA bundle file."
        ),
    )
    vault_auth_mount_point: str | None = Field(
        default=None,
        examples=["approle", "kubernetes", "custom-auth"],
        description=(
            "Custom mount point for the authentication method."
            + " If not specified, defaults to the standard mount point for the auth method."
        ),
    )
    service_account_token_path: Path = Field(
        default=Path("/var/run/secrets/kubernetes.io/serviceaccount/token"),
        examples=[
            "/var/run/secrets/kubernetes.io/serviceaccount/token",
            "/custom/path/to/token",
        ],
        description=(
            "Path to the Kubernetes service account token file."
            + " Used for Kubernetes authentication."
        ),
    )

    @field_validator("vault_verify")
    @classmethod
    def validate_vault_ca(cls, value: bool | str) -> bool | str:
        """Check that the CA bundle can be read if it is specified."""
        if isinstance(value, str):
            path = Path(value)
            if not path.exists():
                raise ValueError(f"Vault CA bundle not found at: {path}")
            try:
                bundle = path.open().read()
            except OSError as error:
                raise ValueError("Vault CA bundle cannot be read") from error
            if "-----BEGIN CERTIFICATE-----" not in bundle:
                raise ValueError("Vault CA bundle does not contain a certificate")
        return value
