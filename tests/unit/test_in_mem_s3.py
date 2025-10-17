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
"""Tests for the mock S3 test fixture"""

import pytest
from pydantic import SecretStr

from hexkit.providers.s3 import S3Config
from hexkit.providers.testing.s3 import InMemObjectStorage

config = S3Config(
    s3_endpoint_url="",
    s3_access_key_id="",
    s3_secret_access_key=SecretStr(""),
    s3_session_token=None,
    aws_config_ini=None,
)

pytestmark = pytest.mark.asyncio()

BUCKET1 = "bucket1"
OBJECT1 = "object1"


async def test_create_bucket_and_does_bucket_exist():
    """Test the functionality of `.create_bucket()` and `.does_bucket_exist()`"""
    s3 = InMemObjectStorage(config=config)
    assert s3.buckets == {}
    assert not await s3.does_bucket_exist(BUCKET1)
    await s3.create_bucket(BUCKET1)
    assert BUCKET1 in s3.buckets
    assert await s3.does_bucket_exist(BUCKET1)


async def test_adding_bucket_that_exists():
    """Make sure an error is raised when trying to create a bucket that already exists"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.BucketAlreadyExistsError):
        await s3.create_bucket(BUCKET1)
    assert s3.buckets == {BUCKET1: set()}


async def test_does_object_exist():
    """Check the functionality of `.does_object_exist()`"""
    s3 = InMemObjectStorage(config=config)
    assert s3.buckets == {}
    assert not await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)
    s3.buckets[BUCKET1].add(OBJECT1)
    assert s3.buckets == {BUCKET1: {OBJECT1}}
    assert await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_init_multipart_upload():
    """Test initiating a multipart upload and verifying upload ID format"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    await s3.create_bucket(bucket_id=BUCKET1)
    await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    uploads: set[str] = s3.uploads[BUCKET1].get(OBJECT1, set())
    upload_id = next(iter(uploads))
    assert upload_id.count("-") == 4


async def test_abort_multipart_upload():
    """Test aborting a multipart upload and verifying it's removed from active uploads"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(bucket_id=BUCKET1)
    upload_id = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Verify upload exists
    assert upload_id in s3.uploads[BUCKET1].get(OBJECT1, {})

    # Abort the upload
    await s3.abort_multipart_upload(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload no longer exists
    assert upload_id not in s3.uploads[BUCKET1].get(OBJECT1, {})


async def test_complete_multipart_upload():
    """Test completing a multipart upload"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(bucket_id=BUCKET1)
    upload_id = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Verify upload exists
    assert upload_id in s3.uploads[BUCKET1].get(OBJECT1, {})

    # Verify object doesn't exist yet
    assert not await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    # Complete the upload
    await s3.complete_multipart_upload(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload no longer exists and object now exists
    assert upload_id not in s3.uploads[BUCKET1].get(OBJECT1, {})
    assert await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_delete_object():
    """Test deleting an object"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(bucket_id=BUCKET1)

    # Try to delete non-existent object - should raise error
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await s3.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object and then delete it
    s3.buckets[BUCKET1].add(OBJECT1)
    assert await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    await s3.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)
    assert not await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_delete_bucket():
    """Test deleting a bucket"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(bucket_id=BUCKET1)

    # Manually add an object to the bucket
    s3.buckets[BUCKET1].add(OBJECT1)

    # Try to delete non-empty bucket without delete_content flag - should fail
    with pytest.raises(InMemObjectStorage.BucketNotEmptyError):
        await s3.delete_bucket(BUCKET1)

    # Delete with delete_content=True should work
    await s3.delete_bucket(BUCKET1, delete_content=True)
    assert not await s3.does_bucket_exist(BUCKET1)

    # Test deleting empty bucket
    await s3.create_bucket(bucket_id=BUCKET1)
    await s3.delete_bucket(BUCKET1)  # Should work on empty bucket
    assert not await s3.does_bucket_exist(BUCKET1)


async def test_upload_object_duplicate():
    """Test handling duplicate object uploads"""
    s3 = InMemObjectStorage(config=config)
    await s3.create_bucket(bucket_id=BUCKET1)

    # Manually add an object
    s3.buckets[BUCKET1].add(OBJECT1)
    assert await s3.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    # Try to get upload URL for existing object - should raise error
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await s3.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)
