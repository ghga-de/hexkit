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

"""The core app with some protected methods."""

from typing import Optional

from auth_demo.ports.hangout import HangoutPort
from pydantic import BaseSettings, Field

__all__ = ["Hangout", "HangoutConfig"]


class HangoutConfig(BaseSettings):
    """Config parameters needed for the demo application."""

    greeting: str = Field("Hello", description="How users shall be greeted")
    treat: str = Field("beer", description="What VIPs should be offered")


class Hangout(HangoutPort):
    """Demo application that just shows personalized welcome messages."""

    def __init__(self, *, config: HangoutConfig):
        """Configure relevant ports."""
        self._greeting = config.greeting
        self._treat = config.treat

    async def reception(self, name: Optional[str] = None) -> str:
        """A method that is not protected at all."""
        name = name or "anonymous user"
        return f"{self._greeting}, {name}!"

    async def lobby(self, name: str) -> str:
        """A method that can be accessed only by authenticated users."""
        return f"{self._greeting}, {name}!"

    async def lounge(self, name: str) -> str:
        """A method that can be accessed only by VIP users."""
        return f"{self._greeting}, dear {name}, have a {self._treat}!"
