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
"""Tests for database migrations"""

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pymongo
import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo import IndexModel, MongoClient
from pymongo.collection import Collection

from hexkit.providers.mongodb import MongoDbConfig
from hexkit.providers.mongodb.migrations import (
    MigrationConfig,
    MigrationDefinition,
    MigrationManager,
    MigrationMap,
    Reversible,
)
from hexkit.providers.mongodb.migrations._manager import MigrationTimeoutError
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()

TEST_COLL_NAME = "testCollection"


class V2BasicMigration(MigrationDefinition):
    """Basic migration with minimal functionality and which doesn't copy indexes"""

    version = 2

    async def apply(self):
        """Forward migration function"""
        async with self.auto_finalize(TEST_COLL_NAME, copy_indexes=False):
            await self.migrate_docs_in_collection(
                coll_name=TEST_COLL_NAME,
                change_function=dummy_change_function,
                batch_size=10,
            )


async def dummy_change_function(doc):
    """Does nothing"""
    return doc


class DummyObject(BaseModel):
    """A dummy object that can be used to fill a test DB."""

    title: str
    length: int


async def run_db_migrations(
    config: MigrationConfig, target_version: int, migration_map: MigrationMap
):
    """Test function that will run the migrations specified"""
    async with MigrationManager(
        config=config,
        target_version=target_version,
        migration_map=migration_map,
    ) as mm:
        await mm.migrate_or_wait()


def make_mig_config(
    config: MongoDbConfig,
    db_version_collection: str = "versioningTests",
    migration_wait_sec: int = 1,
    migration_max_wait_sec: int = 1,
) -> MigrationConfig:
    """Create an instance of MigrationConfig from kwargs and MongoDbConfig instance"""
    return MigrationConfig(
        mongo_dsn=config.mongo_dsn,
        db_name=config.db_name,
        mongo_timeout=config.mongo_timeout,
        db_version_collection=db_version_collection,
        migration_wait_sec=migration_wait_sec,
        migration_max_wait_sec=migration_max_wait_sec,
    )


def get_version_coll(client: MongoClient, config: MigrationConfig) -> Collection:
    """Get an instance of the db version collection.

    This returns the synchronous pymongo collection because it is accessed
    through the synchronous pymongo client from the fixture.
    """
    return client[config.db_name][config.db_version_collection]


def assert_not_versioned(version_coll):
    """Boilerplate check to ensure the DB doesn't have versioning initialized"""
    versions = version_coll.find().to_list()
    assert not versions


async def test_v1_init(mongodb: MongoDbFixture):
    """Test that v1 setup is done right.

    At the start, verify that there is versioning collection (or that it's empty).
    Then run the migrations.
    Then check that the collection has been created and has one entry.
    """
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    version_coll = get_version_coll(client, config)
    assert_not_versioned(version_coll)

    # run the migration to set up the versioned DB
    now = datetime.now(tz=timezone.utc)
    await run_db_migrations(config=config, target_version=1, migration_map={})

    # Inspect the results
    versions = version_coll.find().to_list()
    assert len(versions) == 1
    verdoc = versions[0]
    assert verdoc["version"] == 1
    completed = verdoc["completed"]
    assert datetime.fromisoformat(completed) - now < timedelta(seconds=3)
    assert verdoc["migration_type"] == "FORWARD"
    assert isinstance(verdoc["total_duration_ms"], int)


async def test_drop_or_rename_nonexistent_collection(mongodb: MongoDbFixture):
    """Run migrations on a DB with no data in it.

    The migrations should still complete and log the new DB version.
    """
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    version_coll = get_version_coll(client, config)
    assert_not_versioned(version_coll)

    migration_map: MigrationMap = {2: V2BasicMigration}

    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    versions = version_coll.find().to_list()
    assert len(versions) == 2


@pytest.mark.parametrize(
    "indexes",
    [
        [],
        [IndexModel([("title", pymongo.ASCENDING)], name="byTitle")],
        [
            IndexModel([("title", pymongo.ASCENDING)], name="byTitle"),
            IndexModel([("length", pymongo.DESCENDING)], unique=True, name="length"),
        ],
        [
            IndexModel(
                [("title", pymongo.ASCENDING), ("length", pymongo.DESCENDING)],
                unique=True,
                name="compound",
            ),
        ],
    ],
)
async def test_copy_indexes(mongodb: MongoDbFixture, indexes: list[IndexModel]):
    """Test the copy_indexes function"""
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    version_coll = get_version_coll(client, config)
    assert_not_versioned(version_coll)

    # Insert some test data
    collection = client[config.db_name][TEST_COLL_NAME]
    collection.insert_one(DummyObject(title="doc1", length=100).model_dump())
    collection.insert_one(DummyObject(title="doc2", length=200).model_dump())
    collection.insert_one(DummyObject(title="doc3", length=50).model_dump())

    # Create a way to comfortably check created and copied indexes against expected vals
    expected_indexes = []
    for index in indexes:
        doc = index.document
        expected = {}
        expected["name"] = doc["name"]
        expected["key"] = list(doc["key"].items())
        expected["options"] = {k: v for k, v in doc.items() if k not in ["name", "key"]}
        expected_indexes.append(expected)

    # Create an index (or indexes) on the collection
    if indexes:
        collection.create_indexes(indexes)

    # Verify index info
    index_info = collection.index_information()
    created_indexes = [_ for _ in index_info.items()][1:]
    assert len(created_indexes) == len(indexes)
    for expected, (created_name, created_index) in zip(
        expected_indexes, created_indexes
    ):
        assert created_name == expected["name"]
        assert created_index.pop("key") == expected["key"]
        for expected_option, expected_value in expected["options"].items():
            assert created_index[expected_option] == expected_value

    class V2MigrationWithIndexing(MigrationDefinition):
        version = 2

        async def apply(self):
            async with self.auto_finalize(TEST_COLL_NAME, copy_indexes=True):
                await self.migrate_docs_in_collection(
                    coll_name=TEST_COLL_NAME,
                    change_function=dummy_change_function,
                )

    migration_map: MigrationMap = {2: V2MigrationWithIndexing}
    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    # Repeat comparison to confirm the indexes were copied to the new collection
    index_info = collection.index_information()
    copied_indexes = [_ for _ in index_info.items()][1:]
    assert len(copied_indexes) == len(indexes)
    for expected, (copied_name, copied_index) in zip(expected_indexes, copied_indexes):
        assert copied_name == expected["name"]
        assert copied_index.pop("key") == expected["key"]
        for expected_option, expected_value in expected["options"].items():
            assert copied_index[expected_option] == expected_value


async def test_migration_without_copied_index(mongodb: MongoDbFixture):
    """Verify that when a custom index DOES exist but we don't copy it, it doesn't
    appear on the new collection.
    """
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    version_coll = get_version_coll(client, config)
    assert_not_versioned(version_coll)

    # Insert test data
    collection = client[config.db_name][TEST_COLL_NAME]
    collection.insert_one(DummyObject(title="doc1", length=100).model_dump())

    collection.create_index([("title", pymongo.ASCENDING)], name="byTitle")

    # Create the migration class (same as previous test, minus indexing)

    migration_map: MigrationMap = {2: V2BasicMigration}
    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    index_info = collection.index_information()
    assert "byTitle" not in index_info
    assert len(index_info) == 1  # just the ID index


async def test_stage_unstage(mongodb: MongoDbFixture):
    """Stage and immediately unstage a collection with collection name collisions."""
    config = make_mig_config(mongodb.config)
    client: AsyncIOMotorClient = AsyncIOMotorClient(
        str(config.mongo_dsn.get_secret_value())
    )
    db = client.get_database(config.db_name)
    coll_name = "coll1"
    collection = client[config.db_name][coll_name]

    # Insert a dummy doc so our migration has something to do
    await collection.insert_one({"field": "test"})

    async def change_function(doc):
        """Dummy change function for running `migration_docs_in_collection`"""
        return doc

    class TestMig(MigrationDefinition):
        version = 2

        async def apply(self):
            await self.migrate_docs_in_collection(
                coll_name=coll_name,
                change_function=change_function,
            )
            # Create tmp_v2_old_coll1 for name collision upon staging 'coll1'
            # The correct behavior is to drop the collection upon rename if it exists
            temp_coll = client[config.db_name][f"tmp_v2_old_{coll_name}"]
            await temp_coll.insert_one({"some": "document"})
            await self.stage_collection(coll_name)

            # Create tmp_v2_new_coll1 for name collision upon unstaging 'coll1'
            temp_coll = client[config.db_name][f"tmp_v2_new_{coll_name}"]
            await temp_coll.insert_one({"some": "document"})
            await self.unstage_collection(coll_name)

    migdef = TestMig(db=db, is_final_migration=False, unapplying=False)
    await migdef.apply()


async def test_unapply_not_defined(mongodb: MongoDbFixture):
    """Verify that an error is raised when triggering a backward migration
    on a migration definition that doesn't have `unapply()` defined.
    """
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    collection = client[config.db_name][TEST_COLL_NAME]
    collection.insert_one(DummyObject(title="doc1", length=100).model_dump())

    migration_map: MigrationMap = {2: V2BasicMigration}

    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    # Check that we're now set to version 2 in the DB
    version_collection = get_version_coll(client=client, config=config)
    version_docs = version_collection.find().to_list()
    assert len(version_docs) == 2
    assert version_docs[-1]["version"] == 2
    assert version_docs[-1]["migration_type"] == "FORWARD"

    # Now run migrations with v1 as the target, in order to go backward and trigger error
    with pytest.raises(
        RuntimeError,
        match="Planning to unapply migration v2, but it doesn't subclass `Reversible`!",
    ):
        await run_db_migrations(
            config=config, target_version=1, migration_map=migration_map
        )


async def test_successful_unapply(mongodb: MongoDbFixture):
    """Verify that it's possible to unapply/perform reverse migrations"""
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    collection = client[config.db_name][TEST_COLL_NAME]
    collection.insert_one(DummyObject(title="doc1", length=100).model_dump())

    class V2ReversibleMigration(V2BasicMigration, Reversible):
        """Reversible version of the basic migration class"""

        async def unapply(self):
            async with self.auto_finalize(TEST_COLL_NAME, copy_indexes=False):
                await self.migrate_docs_in_collection(
                    coll_name=TEST_COLL_NAME,
                    change_function=dummy_change_function,
                )

    migration_map: MigrationMap = {2: V2ReversibleMigration}

    # Run initial application (init + v2)
    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    # Run to unapply
    await run_db_migrations(
        config=config, target_version=1, migration_map=migration_map
    )

    # Verify that a 3rd record is inserted, reflecting the reversion from v2 to v1
    version_collection = get_version_coll(client=client, config=config)
    version_docs = version_collection.find().to_list()
    assert len(version_docs) == 3
    assert version_docs[-1]["version"] == 1
    assert version_docs[-1]["migration_type"] == "BACKWARD"


async def test_batch_processing(mongodb: MongoDbFixture):
    """Verify that all documents are migrated when the collection is larger than
    the batch size.
    """
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    collection = client[config.db_name][TEST_COLL_NAME]

    # insert a quantity of docs that will require a final insert_many() call.
    #  using 35 because the batch size on our migration is 10 (so 3.5 batches)
    quantity = 35
    for _ in range(quantity):
        collection.insert_one(DummyObject(title="doc1", length=100).model_dump())

    migration_map: MigrationMap = {2: V2BasicMigration}

    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )
    new_collection = client[config.db_name][TEST_COLL_NAME]
    assert new_collection.count_documents(filter={}) == quantity


async def test_migration_idempotence(mongodb: MongoDbFixture):
    """Test that nothing changes when running db version check multiple times"""
    config = make_mig_config(mongodb.config)
    client = mongodb.client
    collection = client[config.db_name][TEST_COLL_NAME]
    collection.insert_one(DummyObject(title="doc1", length=100).model_dump())

    migration_map: MigrationMap = {2: V2BasicMigration}

    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    class CaptureDummy(MigrationDefinition):
        """Class to capture migration calls"""

        version = 2
        apply = AsyncMock()

    migration_map[2] = CaptureDummy

    # run the migration again but with a dummy to capture execution
    await run_db_migrations(
        config=config, target_version=2, migration_map=migration_map
    )

    CaptureDummy.apply.assert_not_called()

    # Check the version records (should only be 2, not 3)
    version_coll = get_version_coll(client, config)
    versions = version_coll.find().sort("completed", 1).to_list()
    assert len(versions) == 2
    assert versions[0]["version"] == 1
    assert versions[1]["version"] == 2


async def test_waiting(mongodb: MongoDbFixture):
    """Test that migrate_or_wait() waits the configured amount of time."""
    config = make_mig_config(mongodb.config)
    assert config.migration_max_wait_sec
    assert 0 < config.migration_max_wait_sec < 5
    async with MigrationManager(
        config=config, target_version=1, migration_map={}
    ) as mm:
        mm._migrate_db = AsyncMock()
        mm._migrate_db.return_value = False  # force it to wait

        start_time = time.perf_counter()
        with pytest.raises(MigrationTimeoutError):
            await mm.migrate_or_wait()
        elapsed = time.perf_counter() - start_time
        assert elapsed > config.migration_max_wait_sec
