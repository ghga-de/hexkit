# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

"""MongoDB-based provider implementing the DaoFactoryProtocol.

Utilities for testing are located in `./testutils.py`.
"""

from functools import partial
from typing import (
    AsyncGenerator,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    overload,
    Generic,
)


from contextlib import asynccontextmanager
from abc import ABC

from pydantic import BaseModel, BaseSettings, Field, SecretStr
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorClientSession,
)

from hexkit.protocols.dao import (
    DaoFactoryProtcol,
    DaoNaturalId,
    DaoSurrogateId,
    Dto,
    DtoCreation,
    DtoCreation_contra,
    TransactionalScope,
)


class MongoDbDaoBase(ABC, Generic[Dto]):
    """A base class with methods common to all MongoDB-based DAOs.
    This shall be used as base class for other MongoDB-based DAO implementations.
    """

    def __init__(
        self,
        *,
        dto_model: Dto,
        id_field: str,
        collection: AsyncIOMotorCollection,
        session: AsyncIOMotorClientSession,
    ):
        """Initialize the DAO.

        Args:
            collection: A collection object from the motor library.
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
        """

        self._collection = collection
        self._session = session
        self._dto_model = dto_model
        self._id_field = id_field

    async def get(self, *, id_: str) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        raise NotImplementedError()

    async def update(self, dto: Dto) -> None:
        """Update an existing resource.

        Args:
            dto:
                The updated resource content as a pydantic-based data transfer object
                including the resource ID.

        Raises:
            ResourceNotFoundError:
                when resource with the id specified in the dto was not found
        """

        raise NotImplementedError()

    async def delete(self, *, id_: str) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        raise NotImplementedError()

    @overload
    async def find(
        self, *, kv: Mapping[str, object], returns: Literal["all"]
    ) -> Sequence[Dto]:
        ...

    @overload
    async def find(
        self,
        *,
        kv: Mapping[str, object],
        returns: Literal["newest", "oldest", "single"],
    ) -> Dto:
        ...

    async def find(
        self,
        *,
        kv: Mapping[str, object],
        returns: Literal["all", "newest", "oldest", "single"] = "all",
    ) -> Union[Sequence[Dto], Dto]:
        """Find resource by specifing a list of key-value pairs that must match.

        Args:
            kv:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields.
            returns:
                Controls the return behavior. Can be one of: "all" - returns all hits;
                "newest" - returns only the resource of the hit list that was inserted
                first); "oldest" - returns only the resource of the hist list that was
                inserted last; "single" - asserts that there will only be one hit
                (will raise an exception otherwise). Defaults to "all".

        Returns:
            If `returns` was set to "all", a sequence of hits is returned. Otherwise will
            return only a single hit. All hits are in the form of the respective DTO
            model.

        Raises:
            NoHitsFoundError:
                Raised when no hits where found when used in "newest", "oldest", or
                "single" mode. When using the "all" mode, zero hits will not cause an
                exception but simply result in an empty list beeing returned.
            MultpleHitsFoundError:
                Raised when obtaining more than one hit when using the "single" mode.
        """

        raise NotImplementedError()


class MongoDbDaoSurrogateId(MongoDbDaoBase[Dto], Generic[Dto, DtoCreation_contra]):
    """A duck type of a DAO that generates an internal/surrogate key for
    indentifying resources in the database. ID/keys cannot be defined by the client of
    the DAO. Thus, both a standard DTO model (first type parameter), which includes
    the key field, as well as special DTO model (second type parameter), which is
    identical to the first one, but does not include the ID field and is dedicated for
     creation of new resources, is needed.
    """

    def __init__(
        self,
        *,
        dto_model: Dto,
        dto_creation_model: DtoCreation_contra,
        id_field: str,
        collection: AsyncIOMotorCollection,
        session: AsyncIOMotorClientSession,
    ):
        """Initialize the DAO.

        Args:
            collection: A collection object from the motor library.
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            dto_creation_model:
                A DTO model specific for creation of a new resource. This
                model has to be identical to the `dto_model` except that it has to miss
                the `id_field`. The resource ID will be generated by the DAO
                implementation upon resource creation.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
        """

    async def insert(self, dto: DtoCreation_contra) -> Dto:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object without the
                resource ID (which will be set automatically).

        Returns:
            Returns a copy of the newly inserted resource including its assigned ID.
        """

        raise NotImplementedError()


class MongoDbDaoNaturalId(MongoDbDaoBase[Dto]):
    """A duck type of a DAO that uses a natural resource ID profided by the client."""

    async def insert(self, dto: Dto) -> None:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.

        Raises:
            ResourceAlreadyExistsError:
                when a resource with the ID specified in the dto does already exist.
        """

        raise NotImplementedError()

    async def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """

        raise NotImplementedError()


Dao = TypeVar("Dao")


@asynccontextmanager
async def mongodb_transactional_scope(
    *,
    partial_dao: partial[Dao],
    client: AsyncIOMotorClient,
) -> AsyncGenerator[Dao, None]:
    """Manages transactional scopes for database interactions using an async context
    manager interface.

    Upon __enter__, opens a new transactional scope. Returns a DAO according to the
    Dao_co type variable."

    Upon __exit__, closes the transactional scope. A full rollback of the transaction is
    performed in case of an exception. Otherwise, the changes to the database are
    committed and flushed.

    Args:
        partial_dao:
            A partially-initialized DAO object that is only missing a `session`
            argument of type motor.motor_asyncio.AsyncIOMotorClientSession.
        client:
            A MongoDB client obkect from the motors library.
    """

    async with await client.start_session() as session:
        async with session.start_transaction():
            yield partial_dao(session=session)


class MongoDbConfig(BaseSettings):
    """Configuration parameters for connecting to a MongoDB server.

    Inherit your config class from this class if your application uses MongoDB."""

    db_connection_str: SecretStr = Field(
        ...,
        example="mongodb://localhost:27017",
        description=(
            "MongoDB connection string. Might include credentials."
            + " For more information see:"
            + " https://naiveskill.com/mongodb-connection-string/"
        ),
    )
    db_name: str = Field(
        ...,
        example="my-database",
        description="Name of the database located on the MongoDB server.",
    )


class MongoDbDaoFactory(DaoFactoryProtcol):
    """A MongoDB-based provider implementing the DaoFactoryProtocol."""

    def __init__(
        self,
        *,
        config: MongoDbConfig,
    ):
        """Initialize the provider with configuration parameter.

        Args:
            config: MongoDB-specific config parameters.
        """

        self._config = config

        # get a database-specific client:
        self._client = AsyncIOMotorClient(self._config.db_connection_str)
        self._db = self._client[self._config.db_name]

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    @overload
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[set[str]] = None,
    ) -> TransactionalScope[DaoNaturalId[Dto]]:
        ...

    @overload
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[set[str]] = None,
    ) -> TransactionalScope[DaoSurrogateId[Dto, DtoCreation]]:
        ...

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]] = None,
        fields_to_index: Optional[set[str]] = None,
    ) -> Union[
        TransactionalScope[DaoSurrogateId[Dto, DtoCreation]],
        TransactionalScope[DaoNaturalId[Dto]],
    ]:
        """Constructs a DAO for interacting with resources in a MongoDB database.

        Please see the DaoFactoryProtcol superclass for documentation of parameters.

        Please note, the method in this MongoDB-specific implementation of the
        DaoFactoryProtcol would not require to be coroutine. However, other
        implementations of the DaoFactoryProtcol might need to perform await responses
        from the database server. Thus for compliance with the DaoFactoryProtcol, this
        method is async.
        """

        if fields_to_index is not None:
            raise NotImplementedError(
                "Indexing on non-ID fields has not been implemented, yet."
            )

        collection = self._db[name]

        # Prepare a partially initialized DAO Object so that the the transaction scope
        # (see below) is only responsible for supplying the missing the session
        # session argument to obtain a fully initialized DAO:
        partial_dao = (
            partial(
                MongoDbDaoNaturalId,
                collection=collection,
                dto_model=dto_model,
                id_field=id_field,
            )
            if dto_creation_model is None
            else partial(
                MongoDbDaoSurrogateId,
                collection=collection,
                dto_model=dto_model,
                id_field=id_field,
            )
        )

        return mongodb_transactional_scope(partial_dao=partial_dao, client=self._client)
