# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Protocol supporting key-value store operations."""

from typing import Generic, Protocol, TypeVar

V = TypeVar("V")


class KeyValueStoreProtocol(Protocol, Generic[V]):
    """A protocol for a key-value store."""

    async def get(self, key: str, default: V | None = None) -> V | None:
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        pass

    async def set(self, key: str, value: V) -> None:
        """Set the value for the given key.

        Providers must reject storing a value of None if they
        support a value domain V that can contain such values.
        """
        pass

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value in the store.
        """
        pass

    async def exists(self, key: str) -> bool:
        """Check if the given key exists.

        Default implementation for classes inheriting from this protocol.
        Providers can use something more efficient.
        """
        return (await self.get(key)) is not None
