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

from unittest.mock import AsyncMock

import pytest

from hexkit.providers.mongodb.migrations import MigrationDefinition


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
