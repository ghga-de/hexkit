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

"""Create access tokens for testing."""

from datetime import datetime
from typing import NamedTuple

from auth_demo.auth.config import AUTH_KEY_PAIR
from jwcrypto import jwt


class UserInfo(NamedTuple):
    """Basic user info"""

    name: str
    is_vip: bool


EXAMPLE_USERS: list[UserInfo] = [
    UserInfo("Ada Lovelace", False),
    UserInfo("Grace Hopper", True),
    UserInfo("Charles Babbage", False),
    UserInfo("Alan Turin", True),
]


def create_token(user: UserInfo) -> str:
    """Create an auth token that can be used for testing."""
    key = AUTH_KEY_PAIR
    header = {"alg": "ES256"}
    iat = int(datetime.now().timestamp())
    exp = iat + 60 * 10  # valid for 10 minutes
    claims = {**user._asdict(), "iat": iat, "exp": exp}
    token = jwt.JWT(header=header, claims=claims)
    token.make_signed_token(key)
    return token.serialize()


def create_example_users():
    """Create a couple of example users for the application."""
    return {
        "users": [
            {**user._asdict(), "token": create_token(user)} for user in EXAMPLE_USERS
        ]
    }
