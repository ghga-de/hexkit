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

"""FastAPI router for the example application."""

from auth_demo.auth.connector import AuthContext, get_auth, require_auth, require_vip
from auth_demo.container import Container  # type: ignore
from auth_demo.ports.hangout import HangoutPort
from auth_demo.users import create_example_users
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends

provide_hangout = Depends(Provide[Container.hangout])

router = APIRouter()


@router.get("/")
async def root():
    """Return a welcome message for testing."""
    return {
        "message": "Hello, world!",
        "endpoints": ["docs", "users", "status", "reception", "lobby", "lounge"],
    }


@router.get("/users")
async def users():
    """Return a list of users with tokens for testing."""
    return create_example_users()


@router.get("/status")
@inject
async def status(
    auth_context: AuthContext = get_auth,
):
    """This endpoint shows the current login status."""
    expires = auth_context.expires.ctime() if auth_context else None
    return {"status": f"logged in until {expires}" if expires else "logged out"}


@router.get("/reception")
@inject
async def reception(
    auth_context: AuthContext = get_auth, hangout: HangoutPort = provide_hangout
):
    """This endpoint is freely available, but personalized."""
    name = auth_context.name if auth_context else None
    return {"message": await hangout.reception(name)}


@router.get("/lobby")
@inject
async def protected(
    auth_context: AuthContext = require_auth, hangout: HangoutPort = provide_hangout
):
    """This endpoint requires authentication."""
    return {"message": await hangout.lobby(auth_context.name)}


@router.get("/lounge")
@inject
async def admin(
    auth_context: AuthContext = require_vip, hangout: HangoutPort = provide_hangout
):
    """This endpoint requires VIP status."""
    return {"message": await hangout.lounge(auth_context.name)}
