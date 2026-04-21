# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Tests for the mock object storage test fixture"""

from functools import partial
from uuid import uuid4

import pytest

from hexkit.protocols.objstorage import PresignedPostURL
from hexkit.providers.testing.objstorage import InMemObjectStorage

pytestmark = pytest.mark.asyncio()

BUCKET1 = "bucket1"
OBJECT1 = "object1"


async def test_create_bucket_and_does_bucket_exist():
    """Test the functionality of `.create_bucket()` and `.does_bucket_exist()`"""
    storage = InMemObjectStorage()
    assert storage.buckets == {}
    assert not await storage.does_bucket_exist(BUCKET1)
    await storage.create_bucket(BUCKET1)
    assert BUCKET1 in storage.buckets
    assert await storage.does_bucket_exist(BUCKET1)


async def test_adding_bucket_that_exists():
    """Make sure an error is raised when trying to create a bucket that already exists"""
    storage = InMemObjectStorage()
    await storage.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.BucketAlreadyExistsError):
        await storage.create_bucket(BUCKET1)
    assert storage.buckets == {BUCKET1: set()}


async def test_does_object_exist():
    """Check the functionality of `.does_object_exist()`"""
    storage = InMemObjectStorage()
    assert storage.buckets == {}
    assert not await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)
    storage.buckets[BUCKET1].add(OBJECT1)
    assert storage.buckets == {BUCKET1: {OBJECT1}}
    assert await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_delete_object():
    """Test deleting an object"""
    storage = InMemObjectStorage()
    await storage.create_bucket(bucket_id=BUCKET1)

    # Try to delete non-existent object - should raise error
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await storage.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object and then delete it
    storage.buckets[BUCKET1].add(OBJECT1)
    assert await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    await storage.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)
    assert not await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_delete_bucket():
    """Test deleting a bucket"""
    storage = InMemObjectStorage()
    await storage.create_bucket(bucket_id=BUCKET1)

    # Manually add an object to the bucket
    storage.buckets[BUCKET1].add(OBJECT1)

    # Try to delete non-empty bucket without delete_content flag - should fail
    with pytest.raises(InMemObjectStorage.BucketNotEmptyError):
        await storage.delete_bucket(BUCKET1)

    # Delete with delete_content=True should work
    await storage.delete_bucket(BUCKET1, delete_content=True)
    assert not await storage.does_bucket_exist(BUCKET1)

    # Test deleting empty bucket
    await storage.create_bucket(bucket_id=BUCKET1)
    await storage.delete_bucket(BUCKET1)  # Should work on empty bucket
    assert not await storage.does_bucket_exist(BUCKET1)


async def test_upload_object_duplicate():
    """Test handling duplicate object uploads"""
    storage = InMemObjectStorage()
    await storage.create_bucket(bucket_id=BUCKET1)

    # Manually add an object
    storage.buckets[BUCKET1].add(OBJECT1)
    assert await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    # Try to get upload URL for existing object - should raise error
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await storage.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_list_all_object_ids():
    """Test the `.list_all_object_ids()` method"""
    storage = InMemObjectStorage()
    # Check for error if bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.list_all_object_ids(BUCKET1)

    await storage.create_bucket(bucket_id=BUCKET1)

    # Manually add some objects
    object_ids = {str(uuid4()) for _ in range(5)}
    for object_id in object_ids:
        storage.buckets[BUCKET1].add(object_id)

    listed_ids = await storage.list_all_object_ids(BUCKET1)
    assert sorted(listed_ids) == sorted(object_ids)


async def test_assert_object_exists():
    """Test the `._assert_object_exists()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    await storage.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await storage._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object
    storage.buckets[BUCKET1].add(OBJECT1)
    await storage._assert_object_exists(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_assert_object_not_exists():
    """Test the `._assert_object_not_exists()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    await storage.create_bucket(BUCKET1)
    await storage._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)

    # Manually add object
    storage.buckets[BUCKET1].add(OBJECT1)
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await storage._assert_object_not_exists(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_get_object_upload_url():
    """Test the `.get_object_upload_url()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        url = await storage.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add the bucket and an object to trigger the other error type
    await storage.create_bucket(BUCKET1)
    storage.buckets[BUCKET1].add(OBJECT1)
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        url = await storage.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Delete the object and try the method again for success
    await storage.delete_object(bucket_id=BUCKET1, object_id=OBJECT1)
    url = await storage.get_object_upload_url(bucket_id=BUCKET1, object_id=OBJECT1)
    assert isinstance(url, PresignedPostURL)
    assert url.url == f"https://storage.test/{BUCKET1}/{OBJECT1}"


async def test_multipart_upload_for_object():
    """Test the `._list_multipart_upload_for_object()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        uploads = await storage._list_multipart_upload_for_object(
            bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Create bucket
    await storage.create_bucket(BUCKET1)
    uploads = await storage._list_multipart_upload_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == []

    # Create upload
    upload_id = str(uuid4())
    storage.uploads[BUCKET1][OBJECT1] = set()
    storage.uploads[BUCKET1][OBJECT1].add(upload_id)
    uploads = await storage._list_multipart_upload_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == [upload_id]


async def test_assert_no_multipart_upload():
    """Test the `._assert_no_multipart_upload()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and check
    await storage.create_bucket(BUCKET1)
    await storage._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create upload and check for error
    upload_id = str(uuid4())
    storage.uploads[BUCKET1][OBJECT1] = set()
    storage.uploads[BUCKET1][OBJECT1].add(upload_id)
    with pytest.raises(InMemObjectStorage.MultiPartUploadAlreadyExistsError):
        await storage._assert_no_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_assert_multipart_upload_exists():
    """Test the `._assert_multipart_upload_exists()` method"""
    storage = InMemObjectStorage()
    upload_id = str(uuid4())
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Create bucket and check for 'no upload' error
    await storage.create_bucket(BUCKET1)
    with pytest.raises(InMemObjectStorage.MultiPartUploadNotFoundError):
        await storage._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Add one upload and check -- shouldn't get an error
    storage.uploads[BUCKET1][OBJECT1] = set()
    storage.uploads[BUCKET1][OBJECT1].add(upload_id)
    await storage._assert_multipart_upload_exists(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Add another upload -- should now get error about multiple uploads only if
    #  assert_exclusiveness is True (default)
    storage.uploads[BUCKET1][OBJECT1].add(str(uuid4()))
    with pytest.raises(InMemObjectStorage.MultipleActiveUploadsError):
        await storage._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Without that setting, no error appears:
    await storage._assert_multipart_upload_exists(
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
        assert_exclusiveness=False,
    )


async def test_init_multipart_upload():
    """Test initiating a multipart upload and verifying upload ID format"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    await storage.create_bucket(bucket_id=BUCKET1)
    await storage.init_multipart_upload(bucket_id=BUCKET1, object_id=OBJECT1)
    uploads: set[str] = storage.uploads[BUCKET1].get(OBJECT1, set())
    upload_id = next(iter(uploads))
    assert upload_id.count("-") == 4


async def test_get_part_upload_url():
    """Test the `.get_part_upload_url()` method"""
    storage = InMemObjectStorage()
    upload_id = str(uuid4())
    fn = partial(
        storage.get_part_upload_url,
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
    )

    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn(part_number=1)

    # Create bucket
    await storage.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.MultiPartUploadNotFoundError):
        await fn(part_number=1)

    # Init the upload
    upload_id = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Make partial function with actual upload ID now
    fn = partial(
        storage.get_part_upload_url,
        upload_id=upload_id,
        bucket_id=BUCKET1,
        object_id=OBJECT1,
    )
    url = await fn(part_number=1)
    assert url == (
        f"https://storage.test/{BUCKET1}/{OBJECT1}/{upload_id}"
        + "/part_no_1?expires_after=3600"
    )

    with pytest.raises(ValueError):  # Negative part number
        url = await fn(part_number=-1)

    with pytest.raises(ValueError):  # Giant part number
        url = await fn(part_number=92183498)

    # Add another upload to trigger multiple uploads error
    storage.uploads[BUCKET1][OBJECT1].add(str(uuid4()))
    with pytest.raises(InMemObjectStorage.MultipleActiveUploadsError):
        url = await fn(part_number=1)


async def test_abort_multipart_upload():
    """Test aborting a multipart upload and verifying it's removed from active uploads"""
    storage = InMemObjectStorage()
    await storage.create_bucket(bucket_id=BUCKET1)
    upload_id = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload exists
    assert upload_id in storage.uploads[BUCKET1].get(OBJECT1, {})

    # Abort the upload
    await storage.abort_multipart_upload(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload no longer exists
    assert upload_id not in storage.uploads[BUCKET1].get(OBJECT1, {})


async def test_complete_multipart_upload():
    """Test completing a multipart upload"""
    storage = InMemObjectStorage()
    await storage.create_bucket(bucket_id=BUCKET1)
    upload_id = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload exists
    assert upload_id in storage.uploads[BUCKET1].get(OBJECT1, {})

    # Verify object doesn't exist yet
    assert not await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)

    # Complete the upload
    await storage.complete_multipart_upload(
        upload_id=upload_id, bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Verify upload no longer exists and object now exists
    assert upload_id not in storage.uploads[BUCKET1].get(OBJECT1, {})
    assert await storage.does_object_exist(bucket_id=BUCKET1, object_id=OBJECT1)


async def test_get_object_download_url():
    """Test the `.get_object_download_url()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await storage.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await storage.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    storage.buckets[BUCKET1].add(OBJECT1)
    url = await storage.get_object_download_url(bucket_id=BUCKET1, object_id=OBJECT1)
    assert url == f"https://storage.test/{BUCKET1}/{OBJECT1}?expires_after=86400"


async def test_get_object_etag():
    """Test the `.get_object_etag()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await storage.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await storage.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    storage.buckets[BUCKET1].add(OBJECT1)
    etag = await storage.get_object_etag(bucket_id=BUCKET1, object_id=OBJECT1)
    assert etag == f"etag_for_{OBJECT1}"


async def test_get_object_size():
    """Test the `.get_object_size()` method"""
    storage = InMemObjectStorage()
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)

    # Create bucket and expect ObjectNotFoundError
    await storage.create_bucket(bucket_id=BUCKET1)
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await storage.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)

    # Add object and test for success
    storage.buckets[BUCKET1].add(OBJECT1)
    size = await storage.get_object_size(bucket_id=BUCKET1, object_id=OBJECT1)
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

    storage = InMemObjectStorage()
    fn = partial(
        storage.copy_object,
        source_bucket_id=source_bucket_id,
        source_object_id=source_object_id,
        dest_bucket_id=dest_bucket_id,
        dest_object_id=dest_object_id,
    )

    # Trigger an error due to the source bucket not existing
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn()

    # Add the source bucket
    await storage.create_bucket(bucket_id=source_bucket_id)

    # Trigger an error due to the dest bucket not existing
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await fn()

    # Create the dest bucket
    await storage.create_bucket(bucket_id=dest_bucket_id)

    # Trigger an error due to the source object not existing
    with pytest.raises(InMemObjectStorage.ObjectNotFoundError):
        await fn()

    # Finally copy the object -- should not get an error
    storage.buckets[source_bucket_id].add(source_object_id)
    await fn()

    # Repeat the copy operation to get an ObjectAlreadyExistsError
    with pytest.raises(InMemObjectStorage.ObjectAlreadyExistsError):
        await fn()


async def test_list_multipart_uploads_for_object():
    """Test the `.list_multipart_uploads_for_object()` method"""
    storage = InMemObjectStorage()
    object_id2 = "object2"

    # Should raise error when bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.list_multipart_uploads_for_object(
            bucket_id=BUCKET1, object_id=OBJECT1
        )

    # Create bucket
    await storage.create_bucket(BUCKET1)

    # Should return empty list when no uploads exist for the object
    uploads = await storage.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == []

    # Create multiple uploads for different objects
    upload_id1 = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    upload_id2 = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=object_id2
    )

    # List uploads for specific object
    uploads = await storage.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert uploads == [upload_id1]

    uploads = await storage.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=object_id2
    )
    assert uploads == [upload_id2]

    # Complete one upload and verify it's removed from the list
    await storage.complete_multipart_upload(
        upload_id=upload_id1, bucket_id=BUCKET1, object_id=OBJECT1
    )
    uploads = await storage.list_multipart_uploads_for_object(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    assert not uploads


async def test_get_all_multipart_uploads():
    """Test the `.get_all_multipart_uploads()` method"""
    storage = InMemObjectStorage()
    bucket2 = "bucket2"
    object_id2 = "object2"
    object_id3 = "object3"

    # Should raise error when bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.get_all_multipart_uploads(bucket_id=BUCKET1)

    # Create buckets
    await storage.create_bucket(BUCKET1)
    await storage.create_bucket(bucket2)

    # Should return empty dict when no uploads exist
    uploads = await storage.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {}

    # Create multiple uploads for different objects
    upload_id1 = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )
    upload_id2 = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=object_id2
    )

    # Upload object 3 to a different bucket
    upload_id3 = await storage.init_multipart_upload(
        bucket_id=bucket2, object_id=object_id3
    )

    # Get all uploads for bucket1
    uploads = await storage.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {upload_id1: OBJECT1, upload_id2: object_id2}

    # Get all uploads for bucket2
    uploads = await storage.get_all_multipart_uploads(bucket_id=bucket2)
    assert uploads == {upload_id3: object_id3}

    # Complete one upload and verify it's removed from the list
    await storage.complete_multipart_upload(
        upload_id=upload_id1, bucket_id=BUCKET1, object_id=OBJECT1
    )
    uploads = await storage.get_all_multipart_uploads(bucket_id=BUCKET1)
    assert uploads == {upload_id2: object_id2}

    # Abort another upload and verify it's removed
    await storage.abort_multipart_upload(
        upload_id=upload_id2, bucket_id=BUCKET1, object_id=object_id2
    )
    assert not await storage.get_all_multipart_uploads(bucket_id=BUCKET1)


async def test_list_parts_normal():
    """Test the dummy implementation of `.list_parts()`"""
    storage = InMemObjectStorage()
    await storage.create_bucket(BUCKET1)
    upload_id = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Default call: returns 3 dummy parts starting at part number 1
    parts = await storage.list_parts(
        bucket_id=BUCKET1, object_id=OBJECT1, upload_id=upload_id
    )
    assert len(parts) == 3
    assert [p["PartNumber"] for p in parts] == [1, 2, 3]

    # max_parts limits the number of results returned
    parts = await storage.list_parts(
        bucket_id=BUCKET1, object_id=OBJECT1, upload_id=upload_id, max_parts=1
    )
    assert len(parts) == 1
    assert parts[0]["PartNumber"] == 1

    # first_part_no shifts the starting part number
    parts = await storage.list_parts(
        bucket_id=BUCKET1, object_id=OBJECT1, upload_id=upload_id, first_part_no=2
    )
    assert len(parts) == 3
    assert [p["PartNumber"] for p in parts] == [2, 3, 4]

    # Combine max_parts and first_part_no together
    parts = await storage.list_parts(
        bucket_id=BUCKET1,
        object_id=OBJECT1,
        upload_id=upload_id,
        max_parts=2,
        first_part_no=3,
    )
    assert len(parts) == 2
    assert [p["PartNumber"] for p in parts] == [3, 4]

    # Each part dict has the expected keys
    for part in parts:
        assert "PartNumber" in part
        assert "Size" in part
        assert "ETag" in part


async def test_list_parts_error():
    """Test the `.list_parts()` method with bad args for max_parts and first_part_no."""
    storage = InMemObjectStorage()

    # Should raise error when bucket doesn't exist
    with pytest.raises(InMemObjectStorage.BucketNotFoundError):
        await storage.list_parts(bucket_id=BUCKET1, object_id="junk", upload_id="junk")

    # Create buckets
    await storage.create_bucket(BUCKET1)

    # Should raise an error with the upload doesn't exist
    with pytest.raises(InMemObjectStorage.MultiPartUploadNotFoundError):
        await storage.list_parts(bucket_id=BUCKET1, object_id="junk", upload_id="junk")

    # Create a multipart upload
    upload_id = await storage.init_multipart_upload(
        bucket_id=BUCKET1, object_id=OBJECT1
    )

    # Try with invalid max_parts
    with pytest.raises(ValueError):
        _ = await storage.list_parts(
            bucket_id=BUCKET1, object_id=OBJECT1, upload_id=upload_id, max_parts=-9
        )

    # Try with negative first_part_no
    with pytest.raises(ValueError):
        _ = await storage.list_parts(
            bucket_id=BUCKET1,
            object_id=OBJECT1,
            upload_id=upload_id,
            first_part_no=-9,
        )

    # Try with first_part_no set to 0
    with pytest.raises(ValueError):
        _ = await storage.list_parts(
            bucket_id=BUCKET1,
            object_id=OBJECT1,
            upload_id=upload_id,
            first_part_no=0,
        )
