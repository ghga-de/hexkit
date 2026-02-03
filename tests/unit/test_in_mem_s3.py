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

from functools import partial
from uuid import uuid4

import pytest
from pydantic import SecretStr

from hexkit.protocols.objstorage import PresignedPostURL
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


async def test_list_all_object_ids():
    """Test the `.list_all_object_ids()` method"""
    s3 = InMemObjectStorage(config=config)
    # Check for error if bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.list_all_object_ids(BUCKET1)

    await s3.create_bucket(bucket_id=BUCKET1)

    # Manually add some objects
    object_ids = {str(uuid4()) for _ in range(5)}
    for object_id in object_ids:
        s3.buckets[BUCKET1].add(object_id)

    listed_ids = await s3.list_all_object_ids(BUCKET1)
    assert sorted(listed_ids) == sorted(object_ids)


async def test_assert_object_exists():
    """Test the `._assert_object_exists()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    await s3.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await s3._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object
    s3.buckets[BUCKET1].add(OBJECT1)
    await s3._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_assert_object_not_exists():
    """Test the `._assert_object_not_exists()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    await s3.create_bucket(BUCKET1)
    await s3._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object
    s3.buckets[BUCKET1].add(OBJECT1)
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await s3._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_get_object_upload_url():
    """Test the `.get_object_upload_url()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        url = await s3.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add the bucket and an object to trigger the other error type
    await s3.create_bucket(BUCKET1)
    s3.buckets[BUCKET1].add(OBJECT1)
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        url = await s3.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Delete the object and try the method again for success
    await s3.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)
    url = await s3.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)
    assert isinstance(url, PresignedPostURL)
    assert url.url == f"https://s3.example.com/{BUCKET1}/{OBJECT1}"


async def test_multipart_upload_for_object():
    """Test the `._list_multipart_upload_for_object()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        uploads = await s3._list_multipart_upload_for_object(
            bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Create bucket
    await s3.create_bucket(BUCKET1)
    uploads = await s3._list_multipart_upload_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == []

    # Create upload
    upload_id = str(uuid4())
    s3.uploads[BUCKET1][OBJECT1] = set()
    s3.uploads[BUCKET1][OBJECT1].add(upload_id)
    uploads = await s3._list_multipart_upload_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == [upload_id]


async def test_assert_no_multipart_upload():
    """Test the `._assert_no_multipart_upload()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and check
    await s3.create_bucket(BUCKET1)
    await s3._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create upload and check for error
    upload_id = str(uuid4())
    s3.uploads[BUCKET1][OBJECT1] = set()
    s3.uploads[BUCKET1][OBJECT1].add(upload_id)
    with pytest.raises(InMemObjectStorage.MultiPartUploadAlreadyExistsError):
        await s3._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_assert_multipart_upload_exists():
    """Test the `._assert_multipart_upload_exists()` method"""
    s3 = InMemObjectStorage(config=config)
    upload_id = str(uuid4())
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Create bucket and check for 'no upload' error
    await s3.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.MultiPartUploadNotFoundError):
        await s3._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Add one upload and check -- shouldn't get an error
    s3.uploads[BUCKET1][OBJECT1] = set()
    s3.uploads[BUCKET1][OBJECT1].add(upload_id)
    await s3._assert_multipart_upload_exists(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Add another upload -- should now get error about multiple uploads only if
    #  assert_exclusiveness is True (default)
    s3.uploads[BUCKET1][OBJECT1].add(str(uuid4()))
    with pytest.raises(InMemObjectStorage.MultipleActiveUploadsError):
        await s3._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Without that setting, no error appears:
    await s3._assert_multipart_upload_exists(
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
        assert_exclusiveness=False,
    )


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


async def test_get_part_upload_url():
    """Test the `.get_part_upload_url()` method"""
    s3 = InMemObjectStorage(config=config)
    upload_id = str(uuid4())
    fn = partial(
        s3.get_part_upload_url,
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
    )

    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn(part_number=1)

    # Create bucket
    await s3.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.MultiPartUploadNotFoundError):
        await fn(part_number=1)

    # Init the upload
    upload_id = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Make partial function with actual upload ID now
    fn = partial(
        s3.get_part_upload_url,
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
    )
    url = await fn(part_number=1)
    assert url == (
        f"https://s3.example.com/{BUCKET1}/{OBJECT1}/{upload_id}"
        + "/part_no_1?expires_after=3600"
    )

    with pytest.raises(ValueError):  # Negative part number
        url = await fn(part_number=-1)

    with pytest.raises(ValueError):  # Giant part number
        url = await fn(part_number=92183498)

    # Add another upload to trigger multiple uploads error
    s3.uploads[BUCKET1][OBJECT1].add(str(uuid4()))
    with pytest.raises(InMemObjectStorage.MultipleActiveUploadsError):
        url = await fn(part_number=1)


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


async def test_get_object_download_url():
    """Test the `.get_object_download_url()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await s3.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await s3.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    s3.buckets[BUCKET1].add(OBJECT1)
    url = await s3.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)
    assert url == f"https://s3.example.com/{BUCKET1}/{OBJECT1}?expires_after=86400"


async def test_get_object_etag():
    """Test the `.get_object_etag()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await s3.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await s3.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    s3.buckets[BUCKET1].add(OBJECT1)
    etag = await s3.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)
    assert etag == f"etag_for_{OBJECT1}"


async def test_get_object_size():
    """Test the `.get_object_size()` method"""
    s3 = InMemObjectStorage(config=config)
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await s3.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await s3.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    s3.buckets[BUCKET1].add(OBJECT1)
    size = await s3.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)
    assert size == 1024


async def test_copy_object():
    """Test the `.copy_object()` method. We ignore the `abort_failed` param because all
    copy operations in the mock are successful as long as there are no issues with the
    bucket or object specified.
    """
    source_bucket_id = BUCKET1
    source_object_id = OBJECT1
    dest_bucket_id = "bucket2"
    dest_object_id = "object_id2"

    s3 = InMemObjectStorage(config=config)
    fn = partial(
        s3.copy_object,
        source_bucket_id=source_bucket_id,
        source_object_id=source_object_id,
        dest_bucket_id=dest_bucket_id,
        dest_object_id=dest_object_id,
    )

    # Trigger an error due to the source bucket not existing
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn()

    # Add the source bucket
    await s3.create_bucket(bucket_id=source_bucket_id)

    # Trigger an error due to the dest bucket not existing
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn()

    # Create the dest bucket
    await s3.create_bucket(bucket_id=dest_bucket_id)

    # Trigger an error due to the source object not existing
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await fn()

    # Finally copy the object -- should not get an error
    s3.buckets[source_bucket_id].add(source_object_id)
    await fn()

    # Repeat the copy operation to get an ObjectAlreadyExistsError
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await fn()


async def test_list_multipart_uploads_for_object():
    """Test the `.list_multipart_uploads_for_object()` method"""
    s3 = InMemObjectStorage(config=config)
    object_id2 = "object2"

    # Should raise error when bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.list_multipart_uploads_for_object(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket
    await s3.create_bucket(BUCKET1)

    # Should return empty list when no uploads exist for the object
    uploads = await s3.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == []

    # Create multiple uploads for different objects
    upload_id1 = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    upload_id2 = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=object_id2)

    # List uploads for specific object
    uploads = await s3.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == [upload_id1]

    uploads = await s3.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=object_id2
    )
    assert uploads == [upload_id2]

    # Complete one upload and verify it's removed from the list
    await s3.complete_multipart_upload(
        upload_id=upload_id1, bucket_id=BUCKET1, object_id=OBJECT1
    )
    uploads = await s3.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert not uploads


async def test_get_all_multipart_uploads():
    """Test the `.get_all_multipart_uploads()` method"""
    s3 = InMemObjectStorage(config=config)
    bucket2 = "bucket2"
    object_id2 = "object2"
    object_id3 = "object3"

    # Should raise error when bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await s3.get_all_multipart_uploads(bucket_id=BUCKET1)

    # Create buckets
    await s3.create_bucket(BUCKET1)
    await s3.create_bucket(bucket2)

    # Should return empty dict when no uploads exist
    uploads = await s3.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {}

    # Create multiple uploads for different objects
    upload_id1 = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    upload_id2 = await s3.init_multipart_upload(bucket_id=BUCKET1, object_id=object_id2)

    # Upload object 3 to a different bucket
    upload_id3 = await s3.init_multipart_upload(bucket_id=bucket2, object_id=object_id3)

    # Get all uploads for bucket1
    uploads = await s3.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {upload_id1: OBJECT1, upload_id2: object_id2}

    # Get all uploads for bucket2
    uploads = await s3.get_all_multipart_uploads(bucket_id=bucket2)
    assert uploads == {upload_id3: object_id3}

    # Complete one upload and verify it's removed from the list
    await s3.complete_multipart_upload(
        upload_id=upload_id1, bucket_id=BUCKET1, object_id=OBJECT1
    )
    uploads = await s3.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {upload_id2: object_id2}

    # Abort another upload and verify it's removed
    await s3.abort_multipart_upload(
        upload_id=upload_id2, bucket_id=BUCKET1, object_id=object_id2
    )
    assert not await s3.get_all_multipart_uploads(bucket_id=BUCKET1)
