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

"""JSON web token based provider implementing the AuthContextProtocol."""

import json
from typing import Any, Optional

from jwcrypto import jwk, jwt
from jwcrypto.common import JWException
from pydantic import BaseSettings, Field, ValidationError

from hexkit.custom_types import JsonObject
from hexkit.protocols.auth import AuthContext_co, AuthContextProtocol

__all__ = ["JWTAuthConfig", "JWTAuthContextProvider"]


class JWTAuthConfig(BaseSettings):
    """JWT based auth specific config params.

    Inherit your config class from this class if you need
    JWT based authentication in the backend.
    """

    auth_key: dict[str, str] = Field(
        ...,
        example={"crv": "P-256", "kty": "EC", "x": "...", "y": "..."},
        description="The public key for validating the token signature.",
    )
    auth_algs: list[str] = Field(
        ["ES256", "RS256"],
        description="A list of all algorithms that can be used for signing tokens.",
    )
    auth_check_claims: dict[str, Any] = Field(
        dict.fromkeys("name email iat exp".split()),
        description="A dict of all claims that shall be verified by the provider."
        + " A value of None means that the claim can have any value.",
    )
    auth_map_claims: dict[str, str] = Field(
        {},
        description="A mapping of claims to attributes in the auth context."
        + " Only differently named attributes must be specified."
        + " The value None can be used to exclude claims from the auth context.",
    )


class JWTAuthContextProvider(AuthContextProtocol[AuthContext_co]):
    """A JWT based provider implementing the AuthContextProtocol."""

    def __init__(self, *, config: JWTAuthConfig, context_class: type[AuthContext_co]):
        """Initialize the provider with the given configuration.

        Raises a JWTAuthConfigError if the configuration is invalid.
        """
        try:
            key = jwk.JWK.from_json(config.auth_key)
            if not key.has_public:
                raise ValueError("No public key found.")
            if key.has_private:
                raise ValueError("Private key found, this should not be added here.")
        except Exception as exc:
            raise self.AuthContextError(
                "No valid token signing key found in the configuration:" f" {exc}"
            ) from exc
        self._key = key
        self._algs = config.auth_algs
        self._check_claims = config.auth_check_claims
        self._map_claims = config.auth_map_claims
        self._context_class = context_class

    async def get_context(self, token: str) -> Optional[AuthContext_co]:
        """Get an authentication and authorization context from a token.

        The token must be a serialized and signed JSON web token.

        Raises an AuthContextValidationError if the provded token cannot
        establish a valid authentication and authorization context.
        """
        claims = dict(self._decode_and_validate_token(token))
        for claim, attribute in self._map_claims.items():
            try:
                value = claims.pop(claim)
            except KeyError as exc:
                raise self.AuthContextValidationError(f"Missing claim {claim}") from exc
            if attribute is not None:
                claims[attribute] = value
        try:
            return self._context_class(**claims)
        except ValidationError as error:
            raise self.AuthContextValidationError(
                f"Invalid auth context: {error}"
            ) from error

    def _decode_and_validate_token(self, token: str) -> JsonObject:
        """Decode and validate the given JSON Web Token.

        Returns the decoded claims in the token as a dictionary if valid.

        Raises a JWTAuthValidationError if the token is invalid.
        """
        if not token:
            raise self.AuthContextValidationError("Empty token")
        try:
            jwt_token = jwt.JWT(
                jwt=token,
                key=self._key,
                algs=self._algs,
                check_claims=self._check_claims,
                expected_type="JWS",
            )
        except (
            JWException,
            UnicodeDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise self.AuthContextValidationError(f"Not a valid token: {exc}") from exc
        try:
            claims = json.loads(jwt_token.claims)
        except json.JSONDecodeError as exc:
            raise self.AuthContextValidationError(
                f"Claims cannot be decoded: {exc}"
            ) from exc
        return claims
