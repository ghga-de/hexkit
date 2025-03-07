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

"""Unit tests for the MongoDB migration tools"""

from unittest.mock import AsyncMock, Mock

import pytest

from hexkit.providers.mongodb.migrations import (
    MigrationDefinition,
    MigrationManager,
    Reversible,
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
    "start, target, expected, migration_type",
    [
        (2, 2, [], "FORWARD"),
        (10, 1, [10, 9, 8, 7, 6, 5, 4, 3, 2], "BACKWARD"),
        (1, 4, [2, 3, 4], "FORWARD"),
        (2, 1, [2], "BACKWARD"),
        (1, 2, [2], "FORWARD"),
    ],
    ids=["SameVer", "Backward10to1", "Forward1to4", "BackwardBy1", "ForwardBy1"],
)
def test_get_version_sequence(
    start: int, target: int, expected: list[int], migration_type: str
):
    """Test the output of MigrationManager._get_version_sequence()

    In the forward case, we don't need to apply current ver
    In the backward case, we don't want to unapply the target ver
    """
    mm = MigrationManager(config=Mock(), target_version=target, migration_map={})
    mm._migration_type = migration_type  # type: ignore
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
    mm._migration_type = "BACKWARD"

    # Dummy migration doesn't have 'unapply', so we expect an error on retrieving it
    with pytest.raises(RuntimeError):
        assert mm._fetch_migration_cls(2) == DummyMigration

    # This class does have unapply(), so no problems expected
    assert mm._fetch_migration_cls(3) == ReversibleMigration

    # Just to be thorough
    with pytest.raises(NotImplementedError):
        mm._fetch_migration_cls(4)
