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

"""Tests for the FederatedS3Fixture and related items."""

import pytest

from hexkit.providers.s3 import S3Config
from hexkit.providers.s3.testutils import (  # noqa: F401
    FederatedS3Fixture,
    federated_s3_fixture,
    s3_multi_container_fixture,
)
from hexkit.providers.s3.testutils._fixtures import S3MultiContainerFixture

pytestmark = pytest.mark.asyncio()

PRIMARY_STORAGE_ALIAS = "primary"
SECONDARY_STORAGE_ALIAS = "secondary"
STORAGE_ALIASES = [PRIMARY_STORAGE_ALIAS, SECONDARY_STORAGE_ALIAS]


@pytest.fixture(scope="session")
def storage_aliases():
    """Return the storage aliases for the federated S3 fixture."""
    return STORAGE_ALIASES


async def test_get_configs_by_alias(federated_s3: FederatedS3Fixture):
    """Test that `get_configs_by_alias` returns the configs of each storage by name."""
    configs = federated_s3.get_configs_by_alias()
    assert configs.keys() == set(STORAGE_ALIASES)
    for config in configs.values():
        assert isinstance(config, S3Config)


async def test_populate_dummy_items(federated_s3: FederatedS3Fixture):
    """Test the populate_dummy_items function on the FederatedS3Fixture."""
    # Define some stuff to add
    buckets = {
        "bucket1": ["object1", "object2"],
        "empty": [],
    }

    # Populate the items
    await federated_s3.populate_dummy_items(PRIMARY_STORAGE_ALIAS, buckets)

    # Check that the items were added to the primary storage
    assert await federated_s3.storages[PRIMARY_STORAGE_ALIAS].storage.does_object_exist(
        bucket_id="bucket1", object_id="object1"
    )
    assert await federated_s3.storages[PRIMARY_STORAGE_ALIAS].storage.does_bucket_exist(
        bucket_id="empty"
    )

    # Check that the items were not added to/are not accessible via the secondary storage
    assert not await federated_s3.storages[
        SECONDARY_STORAGE_ALIAS
    ].storage.does_object_exist(bucket_id="bucket1", object_id="object1")
    assert not await federated_s3.storages[
        SECONDARY_STORAGE_ALIAS
    ].storage.does_bucket_exist(bucket_id="empty")


async def test_multi_container_fixture(
    s3_multi_container: S3MultiContainerFixture,
):
    """Test that the multi container fixture actually uses separate S3 instances."""
    assert s3_multi_container.s3_containers.keys() == set(STORAGE_ALIASES)
    storage1 = s3_multi_container.s3_containers[PRIMARY_STORAGE_ALIAS]
    storage2 = s3_multi_container.s3_containers[SECONDARY_STORAGE_ALIAS]
    assert storage1.s3_config != storage2.s3_config
