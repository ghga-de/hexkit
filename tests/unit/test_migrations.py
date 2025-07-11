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

"""Unit tests for the MongoDB migration tools"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError

from hexkit.providers.mongodb.migrations import (
    MigrationDefinition,
    MigrationManager,
    Reversible,
    validate_doc,
)
from hexkit.providers.mongodb.migrations.helpers import (
    convert_outbox_correlation_id_v6,
    convert_persistent_event_v6,
    convert_uuids_and_datetimes_v6,
)


class DummyMigration(MigrationDefinition):
    """Just a dummy migration class"""

    version = 2
    apply = AsyncMock()


@pytest.mark.parametrize("unapplying", [True, False])
def test_new_temp_name(unapplying: bool):
    """Unit test for the 'new_temp_name' method on MigrationDefinition"""
    dm = DummyMigration(db=AsyncMock(), unapplying=unapplying, is_final_migration=False)
    expected = f"tmp_v2{'_unapply' if unapplying else ''}_new_sample"
    assert dm.new_temp_name("sample") == expected


@pytest.mark.parametrize("unapplying", [True, False])
def test_old_temp_name(unapplying: bool):
    """Unit test for the 'old_temp_name' method on MigrationDefinition"""
    dm = DummyMigration(db=AsyncMock(), unapplying=unapplying, is_final_migration=False)
    expected = f"tmp_v2{'_unapply' if unapplying else ''}_old_sample"
    assert dm.old_temp_name("sample") == expected


@pytest.mark.parametrize("unapplying", [True, False])
def test_get_new_temp_names(unapplying: bool):
    """Unit test for the 'get_new_temp_names' method on MigrationDefinition"""
    dm = DummyMigration(db=AsyncMock(), unapplying=unapplying, is_final_migration=False)
    names = ["sample1", "sample2", "sample3"]
    multiple = [f"tmp_v2{'_unapply' if unapplying else ''}_new_{n}" for n in names]
    assert dm.get_new_temp_names(names) == multiple


@pytest.mark.parametrize("unapplying", [True, False])
def test_get_old_temp_names(unapplying: bool):
    """Unit test for the 'get_old_temp_names' method on MigrationDefinition"""
    dm = DummyMigration(db=AsyncMock(), unapplying=unapplying, is_final_migration=False)
    names = ["sample1", "sample2", "sample3"]
    multiple = [f"tmp_v2{'_unapply' if unapplying else ''}_old_{n}" for n in names]
    assert dm.get_old_temp_names(names) == multiple


@pytest.mark.parametrize(
    "start, target, expected, backward",
    [
        (2, 2, [], False),
        (10, 1, [10, 9, 8, 7, 6, 5, 4, 3, 2], True),
        (1, 4, [2, 3, 4], False),
        (2, 1, [2], True),
        (1, 2, [2], False),
    ],
    ids=["SameVer", "Backward10to1", "Forward1to4", "BackwardBy1", "ForwardBy1"],
)
def test_get_version_sequence(
    start: int, target: int, expected: list[int], backward: bool
):
    """Test the output of MigrationManager._get_version_sequence()

    In the forward case, we don't need to apply current ver
    In the backward case, we don't want to unapply the target ver
    """
    mm = MigrationManager(config=Mock(), target_version=target, migration_map={})
    mm._backward = backward
    seq = mm._get_version_sequence(current_ver=start)
    assert seq == expected


def test_fetch_migration_cls():
    """Verify the behavior of MigrationManager._fetch_migration_cls()"""

    class ReversibleMigration(MigrationDefinition, Reversible):
        """Reversible Dummy"""

        version = 3
        apply = AsyncMock()
        unapply = AsyncMock()

    migration_map = {2: DummyMigration, 3: ReversibleMigration}
    mm = MigrationManager(config=Mock(), target_version=2, migration_map=migration_map)
    assert mm._fetch_migration_cls(2) == DummyMigration
    assert mm._fetch_migration_cls(3) == ReversibleMigration

    # No migration for v4 implemented
    with pytest.raises(NotImplementedError):
        mm._fetch_migration_cls(4)

    # set migration in reverse mode
    mm._backward = True

    # Dummy migration doesn't have 'unapply', so we expect an error on retrieving it
    with pytest.raises(RuntimeError):
        assert mm._fetch_migration_cls(2) == DummyMigration

    # This class does have unapply(), so no problems expected
    assert mm._fetch_migration_cls(3) == ReversibleMigration

    # Just to be thorough
    with pytest.raises(NotImplementedError):
        mm._fetch_migration_cls(4)


def test_validate_doc():
    """Check that `validate_doc` does what it's supposed to."""

    class DummyObject(BaseModel):
        """A dummy object that can be used to test model/doc validation."""

        title: str
        length: int

    # Happy path
    doc = {"_id": "Test Title", "length": 100}
    doc_copy = {**doc}
    id_field = "title"
    validate_doc(doc=doc, model=DummyObject, id_field=id_field)

    # Verify the function didn't modify the input
    assert doc == doc_copy

    # Migrated document missing required field
    invalid_doc = {"_id": "Test Title", "bad_field": 100}
    with pytest.raises(ValidationError):
        validate_doc(doc=invalid_doc, model=DummyObject, id_field=id_field)

    # Migrated document contains extra field
    valid_but_wrong_doc = {"_id": "Test Title", "length": 100, "extra_field": "abc"}
    with pytest.raises(RuntimeError):
        validate_doc(doc=valid_but_wrong_doc, model=DummyObject, id_field=id_field)


@pytest.mark.asyncio
async def test_v6_uuid_datetime_conversion():
    """Test the prefab uuid/datetime change function for Hexkit v6."""
    test_uuid = UUID("625f4c14-f09f-4826-beed-9c7a0feee707")
    doc = {
        "_id": "item1",
        "some_uuid": str(test_uuid),
        "some_datetime": "2023-10-01T12:34:56.789123+00:00",
    }

    new_dt = datetime.fromisoformat(doc["some_datetime"])
    new_dt = new_dt.replace(microsecond=new_dt.microsecond // 1000 * 1000)
    expected_doc = {
        "_id": "item1",
        "some_uuid": test_uuid,
        "some_datetime": new_dt,
    }

    # Define the migration
    change_function = convert_uuids_and_datetimes_v6(
        uuid_fields=["some_uuid"],
        date_fields=["some_datetime"],
    )

    assert await change_function(doc) == expected_doc


@pytest.mark.parametrize("compacted", [True, False])
@pytest.mark.asyncio
async def test_v6_persistent_event_updater(compacted: bool, monkeypatch):
    """Test the hexkit v6 migration helper for persistent events."""
    _id = "topic:key" if compacted else "ff6285b8-e440-4e1b-a36c-834d53c53078"
    doc = {
        "_id": _id,
        "created": "2023-10-01T12:34:56.789123+00:00",
        "correlation_id": "625f4c14-f09f-4826-beed-9c7a0feee707",
    }

    new_event_id = UUID("ff6285b8-e440-4e1b-a36c-834d53c53078")
    created = datetime.fromisoformat(doc["created"])
    created = created.replace(microsecond=created.microsecond // 1000 * 1000)
    expected_doc = {
        "_id": _id,
        "created": created,
        "event_id": new_event_id if compacted else UUID(_id),
        "correlation_id": UUID(doc["correlation_id"]),
    }

    # if compacted topic, we have to generate a new random event_id, so patch that:
    if compacted:
        monkeypatch.setattr(
            "hexkit.providers.mongodb.migrations.helpers.uuid4",
            lambda: new_event_id,
        )

    assert await convert_persistent_event_v6(doc) == expected_doc


@pytest.mark.asyncio
async def test_v6_outbox_event_updater():
    """Test the hexkit v6 migration helper for outbox event correlation IDs."""
    doc = {
        "_id": "outbox123",
        "__metadata__": {
            "correlation_id": "625f4c14-f09f-4826-beed-9c7a0feee707",
            "published": False,
            "deleted": False,
        },
    }

    expected_doc = {
        "_id": "outbox123",
        "__metadata__": {
            "correlation_id": UUID("625f4c14-f09f-4826-beed-9c7a0feee707"),
            "published": False,
            "deleted": False,
        },
    }

    assert await convert_outbox_correlation_id_v6(doc) == expected_doc
