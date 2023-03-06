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

"""Configuration of the auth context."""

__all__ = ["AuthContext", "AuthConfig"]

from datetime import datetime
from typing import Any

from auth_demo.util import generate_jwk
from pydantic import BaseModel, Field

from hexkit.providers.jwtauth import JWTAuthConfig


class AuthContext(BaseModel):
    """Example auth context."""

    name: str = Field(..., description="The name of the user")
    expires: datetime = Field(..., description="The expiration date of this context")
    is_vip: bool = Field(False, description="Whether the user is a VIP")


# create a key pair for signing and validating JSON web tokens
AUTH_KEY_PAIR = generate_jwk()


class AuthConfig(JWTAuthConfig):
    """Config parameters and their defaults for the example auth conftext."""

    auth_key = AUTH_KEY_PAIR.export(private_key=False)
    auth_check_claims: dict[str, Any] = {"name": None, "exp": None}
    auth_map_claims: dict[str, str] = {"exp": "expires"}
