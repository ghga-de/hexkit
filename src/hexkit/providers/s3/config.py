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

"""S3-specific configuration."""

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

__all__ = ["S3Config"]


class S3Config(BaseSettings):
    """S3-specific config params.

    Inherit your config class from this class if you need to talk
    to an S3 service in the backend.
    """

    s3_endpoint_url: str = Field(
        ..., examples=["http://localhost:4566"], description="URL to the S3 API."
    )
    s3_access_key_id: str = Field(
        ...,
        examples=["my-access-key-id"],
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    s3_secret_access_key: SecretStr = Field(
        ...,
        examples=["my-secret-access-key"],
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    s3_session_token: SecretStr | None = Field(
        None,
        examples=["my-session-token"],
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    aws_config_ini: Path | None = Field(
        None,
        examples=["~/.aws/config"],
        description=(
            "Path to a config file for specifying more advanced S3 parameters."
            + " This should follow the format described here:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file"
        ),
    )
