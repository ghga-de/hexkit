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

"""Connector for using the AuthContext provider with FastAPI."""

from typing import Optional

from auth_demo.auth.config import AuthContext
from auth_demo.container import Container  # type: ignore
from dependency_injector.wiring import Provide, inject
from fastapi import Depends, Security
from fastapi.exceptions import HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.status import HTTP_403_FORBIDDEN

from hexkit.protocols.auth import AuthContextProtocol

__all__ = ["AuthContext", "get_auth", "require_auth", "require_vip"]


@inject
async def get_auth_token(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False)),
    auth_provider: AuthContextProtocol[AuthContext] = Depends(
        Provide[Container.auth_provider]
    ),
) -> Optional[AuthContext]:
    """Get an authentication and authorization context using FastAPI.

    Unauthenticated access is allowed and will return None as auth context.
    """
    token = credentials.credentials if credentials else None
    if not token:
        return None
    try:
        return await auth_provider.get_context(token)
    except AuthContextProtocol.AuthContextError as error:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Invalid authentication credentials",
        ) from error


def get_require_auth_token(vip_only: bool = False):
    """Get a function that retrieves an authentication and authorization context

    Unauthentication access is not allowed and will raise a "Forbidden" error.
    VIP status can be required with the optional "vip_only" flag.
    """

    @inject
    async def require_auth_token(
        credentials: HTTPAuthorizationCredentials = Depends(
            HTTPBearer(auto_error=True)
        ),
        auth_provider: AuthContextProtocol[AuthContext] = Depends(
            Provide[Container.auth_provider]
        ),
    ) -> AuthContext:
        """Get an authentication and authorization context using FastAPI."""

        token = credentials.credentials if credentials else None
        if not token:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Not authenticated"
            )
        try:
            context = await auth_provider.get_context(token)
            if not context:
                raise auth_provider.AuthContextError("Not authenticated")
        except auth_provider.AuthContextError as error:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail="Invalid authentication credentials",
            ) from error
        if vip_only and not context.is_vip:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Only VIPs are authorized here"
            )
        return context

    return require_auth_token


get_auth = Security(get_auth_token)

require_auth = Security(get_require_auth_token())

require_vip = Security(get_require_auth_token(vip_only=True))
