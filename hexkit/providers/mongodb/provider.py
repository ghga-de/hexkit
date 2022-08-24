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

from abc import ABC
from functools import partial
from typing import (
    Any,
    AsyncGenerator,
    Generic,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    overload,
)
from uuid import uuid4

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
)
from pymongo.client_session import ClientSession as AsyncIOMotorClientSession
from pydantic import BaseSettings, Field, SecretStr

from hexkit.protocols.dao import (
    DaoFactoryProtcol,
    DaoNaturalId,
    DaoSurrogateId,
    Dto,
    DtoCreation,
    DtoCreation_contra,
    ResourceNotFoundError,
    TransactionManager,
)

__all__ = ["MongoDbConfig", "MongoDbDaoFactory"]


class MongoDbDaoBase(ABC, Generic[Dto]):
    """A base class with methods common to all MongoDB-based DAOs.
    This shall be used as base class for other MongoDB-based DAO implementations.
    """

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        id_field: str,
        collection: AsyncIOMotorCollection,
        session: AsyncIOMotorClientSession,
    ):
        """Initialize the DAO.

        Args:
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            collection:
                A collection object from the motor library.
            session:
                A AsyncIOMotorClientSession that is within an active transaction.
                Transactions are managed outside of this class.
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

        document = await self._collection.find_one({"_id": id_})

        if document is None:
            raise ResourceNotFoundError(id_=id_)

        # transform the document data into the expected DTO format by renaming the
        # document's primary key (_id) to the expected name of the id_field:
        document[self._id_field] = document.pop("_id")

        return self._dto_model(**document)

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
        dto_model: type[Dto],
        dto_creation_model: type[DtoCreation_contra],
        id_field: str,
        collection: AsyncIOMotorCollection,
        session: AsyncIOMotorClientSession,
    ):
        """Initialize the DAO.

        Args:
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
            collection:
                A collection object from the motor library.
            session:
                A AsyncIOMotorClientSession that is within an active transaction.
                Transactions are managed outside of this class.
        """

        super().__init__(
            dto_model=dto_model,
            id_field=id_field,
            collection=collection,
            session=session,
        )

        self._dto_creation_model = dto_creation_model

    @staticmethod
    def _generate_id() -> str:
        """Generates a new ID for a resource that can be used as primary key in the
        database. It should be assumed that the ID is statistically unique."""

        return str(uuid4())

    async def insert(self, dto: DtoCreation_contra) -> Dto:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object without the
                resource ID (which will be set automatically).

        Returns:
            Returns a copy of the newly inserted resource including its assigned ID.
        """

        # complete the provided data with an autogenerated ID:
        data = dto.dict()
        data[self._id_field] = self._generate_id()

        # verify the completed data against the full DTO model (modifications might be
        # introduced by the pydantic model):
        full_dto = self._dto_model(**data)

        # construct the document that is stored in the DB by using the id_field of the
        # model as primary identifier (_id):
        document = full_dto.dict()
        document["_id"] = document.pop(self._id_field)

        await self._collection.insert_one(document)

        return full_dto


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


class MongoDbTransactionManager(Generic[Dao]):
    """Manages transactional scopes for database interactions using an async context
    manager interface."""

    def __init__(self, *, dao_constructor: partial[Dao], client: AsyncIOMotorClient):
        """Iniatialize the transaction manager with a preconfigured DAO constructor and
        a preconfigured MongoDB client.

        Args:
            dao_constructor:
                A partially-initialized DAO object that is only missing a `session`
                argument of type motor.motor_asyncio.AsyncIOMotorClientSession.
            client:
                A MongoDB client object from the motors library.
        """

        self._dao_constructor = dao_constructor
        self._client = client

        self._session: Optional[AsyncIOMotorClientSession] = None

    async def __aenter__(self) -> Dao:
        """Upon __enter__, opens a new transactional scope. Returns a DAO according to
        the Dao_co type variable."""

        if self._session is not None:
            raise RuntimeError(
                "This transaction manager is already in use. Please close the ongoing"
                + " transaction by running the __aexit__ method."
            )

        self._session = await self._client.start_session()
        self._session.start_transaction()

        return self._dao_constructor(session=self._session)

    async def __aexit__(self, exc_type, exc_value, exc_trace):
        """
        Upon __exit__, closes the transactional scope. A full rollback of the transaction is
        performed in case of an exception. Otherwise, the changes to the database are
        committed and flushed.
        """

        if self._session is None:
            raise RuntimeError("No transaction running, call __aenter__ first.")

        if exc_type is None:
            await self._session.commit_transaction()
        else:
            await self._session.abort_transaction()

        await self._session.end_session()
        self._session = None

        return False


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
        self._client = AsyncIOMotorClient(
            self._config.db_connection_str.get_secret_value()
        )
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
    ) -> TransactionManager[DaoNaturalId[Dto]]:
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
    ) -> TransactionManager[DaoSurrogateId[Dto, DtoCreation]]:
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
        TransactionManager[DaoSurrogateId[Dto, DtoCreation]],
        TransactionManager[DaoNaturalId[Dto]],
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
        # (Using Any because mypy does not (but pylance does) recognize this as
        # Union[partial[MongoDbDaoNaturalId],partial[MongoDbDaoSurrogateId]].)
        partial_dao: Any = (
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
                dto_creation_model=dto_creation_model,
                id_field=id_field,
            )
        )

        return MongoDbTransactionManager(
            dao_constructor=partial_dao, client=self._client
        )
