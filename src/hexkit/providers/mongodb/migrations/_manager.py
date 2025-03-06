# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Tools to run database migrations in services"""

import logging
from asyncio import sleep
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from time import time
from typing import Literal, TypedDict

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import Field
from pymongo.errors import DuplicateKeyError

from hexkit.providers.mongodb import MongoDbConfig

from ._utils import MigrationDefinition, Reversible

log = logging.getLogger(__name__)

MigrationType = Literal["FORWARD", "BACKWARD"]
MigrationCls = type[MigrationDefinition]
MigrationMap = dict[int, MigrationCls]


def now_as_utc() -> datetime:
    """Return the current timestamp with UTC timezone"""
    return datetime.now().astimezone(tz=timezone.utc)


def duration_in_ms(duration: float) -> int:
    return int(duration * 1000)


class MigrationConfig(MongoDbConfig):
    """Minimal configuration required to run the migration process."""

    db_version_collection: str = Field(
        ...,
        description="The name of the collection containing DB version information for this service",
        examples=["ifrsDbVersions"],
    )
    migration_wait_sec: int = Field(
        ...,
        description="The number of seconds to wait before checking the DB version again",
        examples=[5, 30, 180],
    )


class DbVersionRecord(TypedDict):
    """Model containing information about DB versions and how they were achieved."""

    version: int
    completed: str
    migration_type: MigrationType
    total_duration_ms: int


class MigrationStepError(RuntimeError):
    """Raised when a specific migration step fails, e.g. migrating from v4 to v5"""

    def __init__(self, *, current_ver: int, target_ver: int):
        msg = f"Unable to migrate from DB version {current_ver} to {target_ver}."
        super().__init__(msg)


class DbLockError(RuntimeError):
    """Raised when the DB lock can't be released or acquired due to an error."""

    def __init__(
        self, *, op: Literal["acquire", "release"], coll_name: str, err_info: str
    ):
        msg = (
            f"Failed to {op} the lock in collection {coll_name}."
            + f" Error details:\n  '{err_info}'"
        )
        super().__init__(msg)


class DbVersioningInitError(RuntimeError):
    """Raised when DB versioning initialization fails due to an error."""

    def __init__(self, *, err_info: str):
        msg = f"DB versioning initialization failed. Error details:\n  {err_info}"
        super().__init__(msg)


def _get_db_version_from_records(version_docs: list[DbVersionRecord]) -> int:
    """Gets the current DB version from the documents found in the version collection."""
    # Make sure we know what the latest version is, not just the max
    version_docs.sort(key=lambda doc: doc["completed"])
    return version_docs[-1]["version"] if version_docs else 0


class MigrationManager:
    """Top-level logic for ensuring the database is updated before running the service.

    The `migrate_or_wait` method must be called before any instance of the service
    begins its main execution loop.

    Version 1 is reserved for the framework as a way to mark when versioning was added.

    Example usage:
    ```
    from my_service.config import Config  # inherits from MongoDbConfig
    from <module with this class> import MigrationManager
    from <module with migration code> import V2Migration, V3Migration

    DB_VERSION = 2  # the current expected DB version
    MY_MIGRATION_MAP = {2: V2Migration, 3: V3Migration} # etc.

    def migrate_my_service():
        # Called before starting my_service
        config = Config()

        async with MigrationManager(config, DB_VERSION, MY_MIGRATION_MAP) as mm:
            await mm.migrate_or_wait()
    ```
    """

    client: AsyncIOMotorClient
    db: AsyncIOMotorDatabase

    def __init__(
        self,
        config: MigrationConfig,
        target_version: int,
        migration_map: MigrationMap,
    ):
        """Instantiate the MigrationManager.

        Args
        - `config`: Config containing db connection str and lock/db versioning collections
        - `target_version`: Which version the db needs to be at for this version of the service
        - `migration_map`: A dict with the MigrationDefinition class for each db version
        """
        if target_version < 1:
            raise RuntimeError("Expected database version must be 1 or greater")

        self.config = config
        self.target_ver = target_version
        self.migration_map = migration_map
        self._lock_acquired = False
        self._entered = False
        self._migration_type: MigrationType = "FORWARD"

    async def __aenter__(self):
        """Set up database client and database reference"""
        self.client = AsyncIOMotorClient(str(self.config.mongo_dsn.get_secret_value()))
        self.db = self.client[self.config.db_name]
        self._entered = True
        return self

    async def __aexit__(self, exc_type_, exc_value, exc_tb):
        """Release DB lock and close/remove database client"""
        await self._release_db_lock()
        self.client.close()

    async def _get_version_docs(self) -> list[DbVersionRecord]:
        """Gets the DB version information from the database."""
        collection = self.db[self.config.db_version_collection]
        # use a filter to avoid picking up the lock doc, just in case
        version_docs = []
        async for doc in collection.find({"_id": {"$ne": 0}}):
            doc.pop("_id")
            version_docs.append(DbVersionRecord(**doc))  # type: ignore
        return version_docs

    @asynccontextmanager
    async def _lock_db(self):
        await self._acquire_db_lock()
        try:
            yield
        finally:
            await self._release_db_lock()

    async def _acquire_db_lock(self) -> None:
        """Try to acquire the lock on the DB and return the result.

        Logs and raises any error that occurs while updating the lock document.
        """
        if self._lock_acquired:
            log.debug("Database lock already acquired")
            return
        coll_name = self.config.db_version_collection
        try:
            version_coll = self.db[coll_name]
            with suppress(DuplicateKeyError):
                await version_coll.insert_one(
                    {
                        "_id": 0,
                        "lock_acquired": True,
                        "acquired_at": now_as_utc().isoformat(),
                    }
                )
                self._lock_acquired = True
                log.info("Database lock acquired")
        except BaseException as exc:
            error = DbLockError(op="acquire", coll_name=coll_name, err_info=str(exc))
            log.error(error)
            raise error from exc

        if not self._lock_acquired:
            log.debug("Did not acquire DB lock in collection %s", coll_name)

    async def _release_db_lock(self) -> None:
        """Release the DB lock by deleting the lock document.

        Logs and re-raises any errors that occur during the update.
        """
        if not self._lock_acquired:
            log.debug("Database lock already released")
            return
        coll_name = self.config.db_version_collection
        try:
            version_coll = self.db[coll_name]
            await version_coll.find_one_and_delete({"lock_acquired": True})
            self._lock_acquired = False
        except BaseException as exc:
            error = DbLockError(op="release", coll_name=coll_name, err_info=str(exc))
            log.critical(error)
            raise error from exc
        log.info("Database lock released")

    async def _record_migration(self, *, version: int, total_duration_ms: int):
        """Insert a DbVersionRecord with processing information"""
        record = DbVersionRecord(
            version=version,
            completed=now_as_utc().isoformat(),
            migration_type=self._migration_type,
            total_duration_ms=total_duration_ms,
        )
        version_collection = self.db[self.config.db_version_collection]
        await version_collection.insert_one(record)

    async def _initialize_versioning(self) -> bool:
        """Create and acquire the DB lock, then add the versioning collection.

        Returns `True` if setup was performed, else `False`.
        """
        init_start = time()
        async with self._lock_db():
            if self._lock_acquired:
                # Initialize db version collection
                await self._record_migration(
                    version=1,
                    total_duration_ms=duration_in_ms(time() - init_start),
                )
                return True
        return False

    def _get_version_sequence(self, *, current_ver: int) -> list[int]:
        """Return an ordered list of the version migrations to apply/unapply"""
        # In forward case, we don't need to apply current ver
        # in backward case, we don't want to unapply the target ver
        step_range = (
            range(current_ver, self.target_ver, -1)
            if self._migration_type == "BACKWARD"
            else range(current_ver + 1, self.target_ver + 1)
        )
        steps = list(step_range)
        return steps

    def _fetch_migration_cls(self, version: int) -> MigrationCls:
        """Return the stored migration for the specified version.

        Raise an error if the  doesn't exist or doesn't implement unapply when needed.
        """
        try:
            migration_cls = self.migration_map[version]
            if self._migration_type == "BACKWARD" and not issubclass(
                migration_cls, Reversible
            ):
                raise RuntimeError(
                    f"Planning to unapply migration v{version}, but"
                    + f" it doesn't subclass `{Reversible.__name__}`!"
                )
            return migration_cls
        except KeyError as err:
            mig_type = self._migration_type.lower()
            raise NotImplementedError(
                f"No {mig_type} migration implemented for version {version}"
            ) from err

    async def _perform_migrations(self, *, current_ver: int):
        """Migrate forward or backward to reach target DB version.

        Raises `MigrationError` if unsuccessful.
        """
        ver_sequence = self._get_version_sequence(current_ver=current_ver)
        migrations = [self._fetch_migration_cls(ver) for ver in ver_sequence]
        unapplying = self._migration_type == "BACKWARD"
        # Execute & time each migration in order to get to the target DB version
        for version, migration_cls in zip(ver_sequence, migrations):
            try:
                # Determine if this is the last migration to apply/unapply
                is_final_migration = version == ver_sequence[-1]

                # instantiate MigrationDefinition
                migration = migration_cls(
                    db=self.db,
                    unapplying=unapplying,
                    is_final_migration=is_final_migration,
                )

                # Call apply/unapply based on migration type
                await migration.unapply() if unapplying else await migration.apply()
            except BaseException as exc:
                error = MigrationStepError(
                    current_ver=version - 1,
                    target_ver=self.target_ver,
                )
                log.critical(error)
                raise error from exc

    async def _migrate_db(self) -> bool:
        """Ensure the database is up to date before running the actual app.

        If the database is already up to date, no changes are made. If the database is
        out of date, migration code is executed to make the database current.

        Returns True if migrations are finished or up-to-date and False otherwise.
        """
        version_docs = await self._get_version_docs()
        version = _get_db_version_from_records(version_docs)

        if version == 0:
            try:
                init_complete = await self._initialize_versioning()
            except BaseException as exc:
                error = DbVersioningInitError(err_info=str(exc))
                log.critical(error)
                raise error from exc
            if not init_complete:
                return False
            version = 1

        if version == self.target_ver:
            # DB is up to date, run service
            return True

        # DB version is not what it should be: acquire lock and migrate
        async with self._lock_db():
            if not self._lock_acquired:
                return False

            self._migration_type = (
                "FORWARD" if version < self.target_ver else "BACKWARD"
            )

            start = time()
            await self._perform_migrations(current_ver=version)
            duration_ms = duration_in_ms(time() - start)

            # record the db version
            await self._record_migration(
                version=self.target_ver,
                total_duration_ms=duration_ms,
            )
        return True

    async def migrate_or_wait(self):
        """Try to migrate the database or wait until migrations are completed."""
        if not self._entered:
            raise RuntimeError("MigrationManager must be used as a context manager")

        # need to implement some kind of total time limit, warning logging, etc. later
        while not await self._migrate_db():
            await sleep(self.config.migration_wait_sec)
