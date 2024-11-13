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

"""Test S3 storage DAO"""

from contextlib import AbstractContextManager, nullcontext
from typing import Optional

import pytest

from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.providers.s3.testutils import (
    MEBIBYTE,
    FileObject,
    S3Fixture,
    s3_container_fixture,  # noqa: F401
    s3_fixture,  # noqa: F401
    temp_file_object,
    tmp_file,  # noqa: F401
    typical_workflow,
    upload_part,
    upload_part_of_size,
)

EXAMPLE_BUCKETS = [
    "example-bucket-1",
    "example-bucket-2",
]

pytestmark = pytest.mark.asyncio()


async def test_empty_buckets(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """Make sure the empty_buckets() method works."""
    bucket_id, object_id = tmp_file.bucket_id, tmp_file.object_id
    await s3.populate_buckets([bucket_id])
    await s3.populate_file_objects(file_objects=[tmp_file])

    # test empty_buckets() with and without parameters
    await s3.empty_buckets(buckets=[])
    assert await s3.storage.does_object_exist(bucket_id=bucket_id, object_id=object_id)
    await s3.empty_buckets(exclude_buckets=[bucket_id])
    assert await s3.storage.does_object_exist(bucket_id=bucket_id, object_id=object_id)
    await s3.empty_buckets()
    assert not await s3.storage.does_object_exist(
        bucket_id=bucket_id, object_id=object_id
    )


async def test_delete_buckets(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """Make sure the delete_buckets() method works."""
    bucket_id = tmp_file.bucket_id
    await s3.populate_buckets([bucket_id])
    await s3.populate_file_objects(file_objects=[tmp_file])

    # test delete_buckets() with and without parameters
    await s3.delete_buckets(buckets=[])
    assert await s3.storage.does_bucket_exist(bucket_id=bucket_id)
    await s3.delete_buckets(exclude_buckets=[bucket_id])
    assert await s3.storage.does_bucket_exist(bucket_id=bucket_id)
    await s3.delete_buckets()
    assert not await s3.storage.does_bucket_exist(bucket_id=bucket_id)


@pytest.mark.parametrize("use_multipart_upload", [True, False])
async def test_typical_workflow(use_multipart_upload: bool, s3: S3Fixture):
    """Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow."""
    with temp_file_object(size=20 * MEBIBYTE) as file:
        await typical_workflow(
            storage_client=s3.storage,
            bucket1_id=EXAMPLE_BUCKETS[0],
            bucket2_id=EXAMPLE_BUCKETS[1],
            object_id=file.object_id,
            test_file_md5=file.md5,
            test_file_path=file.file_path,
            use_multipart_upload=use_multipart_upload,
        )


async def test_object_existence_checks(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """Test if the checks for existence of objects work correctly."""
    # object should not exist in the beginning:
    assert not await s3.storage.does_object_exist(
        bucket_id=tmp_file.bucket_id, object_id=tmp_file.object_id
    )

    # add the corresponding file object to the storage:
    await s3.populate_file_objects([tmp_file])

    # now the object should exist:
    assert await s3.storage.does_object_exist(
        bucket_id=tmp_file.bucket_id, object_id=tmp_file.object_id
    )


async def test_get_object_size(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """Test if the get_object_size method returns the correct size."""
    expected_size = len(tmp_file.content)

    await s3.populate_file_objects([tmp_file])
    observed_size = await s3.storage.get_object_size(
        bucket_id=tmp_file.bucket_id, object_id=tmp_file.object_id
    )

    assert expected_size == observed_size


async def test_list_all_object_ids(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """Test if listing all object IDs for a bucket works correctly."""
    file_fixture2 = tmp_file.model_copy(deep=True)
    file_fixture2.object_id = "mydefaulttestobject002"

    # add file objects to storage
    await s3.populate_file_objects([tmp_file, file_fixture2])

    # retrieve all object ids
    retrieved_ids = await s3.storage.list_all_object_ids(bucket_id=tmp_file.bucket_id)
    assert retrieved_ids == [tmp_file.object_id, file_fixture2.object_id]


async def test_bucket_existence_checks(s3: S3Fixture):
    """Test if the checks for existence of buckets work correctly."""
    bucket_id = EXAMPLE_BUCKETS[0]

    # bucket should not exist in the beginning:
    assert not await s3.storage.does_bucket_exist(bucket_id=bucket_id)

    # add the corresponding bucket to the storage:
    await s3.populate_buckets([bucket_id])

    # now the bucket should exist:
    assert await s3.storage.does_bucket_exist(bucket_id=bucket_id)


async def test_object_and_bucket_collisions(s3: S3Fixture, tmp_file: FileObject):  # noqa: F811
    """
    Tests whether overwriting (re-creation, re-upload, or copy to existing object)
    fails with the expected error.
    """
    await s3.populate_file_objects([tmp_file])

    with pytest.raises(ObjectStorageProtocol.BucketAlreadyExistsError):
        await s3.storage.create_bucket(tmp_file.bucket_id)

    with pytest.raises(ObjectStorageProtocol.BucketNotEmptyError):
        await s3.storage.delete_bucket(tmp_file.bucket_id)

    with pytest.raises(ObjectStorageProtocol.ObjectAlreadyExistsError):
        await s3.storage.get_object_upload_url(
            bucket_id=tmp_file.bucket_id, object_id=tmp_file.object_id
        )

    with pytest.raises(ObjectStorageProtocol.ObjectAlreadyExistsError):
        await s3.storage.copy_object(
            source_bucket_id=tmp_file.bucket_id,
            source_object_id=tmp_file.object_id,
            dest_bucket_id=tmp_file.bucket_id,
            dest_object_id=tmp_file.object_id,
        )


async def test_handling_non_existing_file_and_bucket(
    s3: S3Fixture,
    tmp_file: FileObject,  # noqa: F811
):
    """
    Tests whether interacting with a non-existing bucket/file object fails with the
    expected result.
    """
    non_existing_bucket_id = "non-existing-bucket-1"
    non_existing_object_id = "non-existing-object-1"
    existing_bucket_id = tmp_file.bucket_id
    existing_object_id = tmp_file.object_id

    await s3.populate_file_objects([tmp_file])

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3.storage.delete_bucket(non_existing_bucket_id)

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3.storage.get_object_download_url(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3.storage.get_object_upload_url(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3.storage.delete_object(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        # copy when source bucket does not exist:
        await s3.storage.copy_object(
            source_bucket_id=non_existing_bucket_id,
            source_object_id=non_existing_object_id,
            dest_bucket_id=existing_bucket_id,
            dest_object_id=existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        # copy when source bucket exists, but source object doesn't:
        await s3.storage.copy_object(
            source_bucket_id=existing_bucket_id,
            source_object_id=non_existing_object_id,
            dest_bucket_id=existing_bucket_id,
            dest_object_id=existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        # copy when destination bucket does not exist:
        await s3.storage.copy_object(
            source_bucket_id=existing_bucket_id,
            source_object_id=existing_object_id,
            dest_bucket_id=non_existing_bucket_id,
            dest_object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        await s3.storage.get_object_size(
            bucket_id=existing_bucket_id, object_id=non_existing_object_id
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        await s3.storage.get_object_download_url(
            bucket_id=existing_bucket_id, object_id=non_existing_object_id
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        await s3.storage.delete_object(
            bucket_id=existing_bucket_id, object_id=non_existing_object_id
        )


@pytest.mark.parametrize("delete_content", (True, False))
async def test_delete_non_empty_bucket(
    delete_content: bool,
    s3: S3Fixture,
    tmp_file: FileObject,  # noqa: F811
):
    """Test deleting a non-empty bucket."""
    await s3.populate_file_objects([tmp_file])

    with (
        nullcontext()
        if delete_content
        else pytest.raises(ObjectStorageProtocol.BucketNotEmptyError)
    ):
        await s3.storage.delete_bucket(
            bucket_id=tmp_file.bucket_id, delete_content=delete_content
        )


@pytest.mark.parametrize(
    "upload_id_correct, bucket_id_correct, object_id_correct, exception",
    [
        (False, True, True, ObjectStorageProtocol.MultiPartUploadNotFoundError),
        (True, False, True, ObjectStorageProtocol.BucketNotFoundError),
        (True, True, False, ObjectStorageProtocol.MultiPartUploadNotFoundError),
    ],
)
async def test_using_non_existing_upload(
    upload_id_correct: bool,
    bucket_id_correct: bool,
    object_id_correct: bool,
    exception,
    s3: S3Fixture,
):
    """
    Makes sure that using a non existing upload_id-bucket_id-object_id combination
    throws the right error.
    """
    # prepare a non-completed upload:
    (
        real_upload_id,
        real_bucket_id,
        real_object_id,
    ) = await s3.prepare_non_completed_upload()

    upload_id = real_upload_id if upload_id_correct else "wrong-upload"
    bucket_id = real_bucket_id if bucket_id_correct else "wrong-bucket"
    object_id = real_object_id if object_id_correct else "wrong-object"

    def get_exception_context() -> AbstractContextManager:
        return pytest.raises(exception) if exception else nullcontext()

    # call relevant methods from the provider:
    with pytest.raises(exception):
        await s3.storage._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

    with pytest.raises(exception):
        await s3.storage.get_part_upload_url(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id, part_number=1
        )

    with pytest.raises(exception):
        await s3.storage.complete_multipart_upload(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )


@pytest.mark.parametrize(
    "part_number, exception",
    [(0, ValueError), (1, None), (10000, None), (10001, ValueError)],
)
async def test_invalid_part_number(
    part_number: int, exception: Optional[type[Exception]], s3: S3Fixture
):
    """Check that invalid part numbers are cached correctly."""
    upload_id, bucket_id, object_id = await s3.prepare_non_completed_upload()

    with pytest.raises(exception) if exception else nullcontext():
        _ = await s3.storage.get_part_upload_url(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            part_number=part_number,
        )


@pytest.mark.parametrize(
    "part_sizes, anticipated_part_size, anticipated_part_quantity, exception",
    [
        ([10 * MEBIBYTE, 10 * MEBIBYTE, 1 * MEBIBYTE], None, None, None),
        ([10 * MEBIBYTE, 10 * MEBIBYTE, 1 * MEBIBYTE], 10 * MEBIBYTE, 3, None),
        (
            [],
            None,
            None,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # zero parts uploaded
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            None,
            2,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # Mismatch with anticipated parts
        (
            [10 * MEBIBYTE, 5 * MEBIBYTE, 1 * MEBIBYTE],
            None,
            None,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # heterogenous part sizes
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            None,
            None,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # last part bigger than first part
        (
            [10 * MEBIBYTE, 5 * MEBIBYTE, 1 * MEBIBYTE],
            10 * MEBIBYTE,
            None,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # mismatch anticipated part size
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            10 * MEBIBYTE,
            None,
            ObjectStorageProtocol.MultiPartUploadConfirmError,
        ),  # Too large last part
    ],
)
async def test_complete_multipart_upload(
    part_sizes: list[int],
    anticipated_part_size: Optional[int],
    anticipated_part_quantity: Optional[int],
    exception: Optional[type[Exception]],
    s3: S3Fixture,
):
    """Test the complete_multipart_upload method."""
    upload_id, bucket_id, object_id = await s3.get_initialized_upload()
    for part_idx, part_size in enumerate(part_sizes):
        await upload_part_of_size(
            storage_dao=s3.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            size=part_size,
            part_number=part_idx + 1,
        )

    with pytest.raises(exception) if exception else nullcontext():
        await s3.storage.complete_multipart_upload(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            anticipated_part_quantity=anticipated_part_quantity,
            anticipated_part_size=anticipated_part_size,
        )


@pytest.mark.parametrize("empty_upload", (True, False))
async def test_abort_multipart_upload(empty_upload: bool, s3: S3Fixture):
    """Test the abort_multipart_upload method."""
    upload_id, bucket_id, object_id = await s3.get_initialized_upload()

    async def upload_part_shortcut(part_number):
        await upload_part_of_size(
            storage_dao=s3.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            size=ObjectStorageProtocol.DEFAULT_PART_SIZE,
            part_number=part_number,
        )

    if not empty_upload:
        # upload 2 parts:
        for part_number in range(1, 3):
            await upload_part_shortcut(part_number)

    await s3.storage.abort_multipart_upload(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
    )

    # verify that the upload cannot be continued:
    with pytest.raises(ObjectStorageProtocol.MultiPartUploadNotFoundError):
        await upload_part_shortcut(part_number=3)

    # ... and also not restarted:
    with pytest.raises(ObjectStorageProtocol.MultiPartUploadNotFoundError):
        await upload_part_shortcut(part_number=1)


async def test_multiple_active_uploads(s3: S3Fixture):
    """Test that multiple active uploads for the same object are not possible."""
    # initialize an upload:
    _, bucket_id, object_id = await s3.get_initialized_upload()

    # initialize another upload for the same object:
    with pytest.raises(ObjectStorageProtocol.MultiPartUploadAlreadyExistsError):
        _ = await s3.storage.init_multipart_upload(
            bucket_id=bucket_id, object_id=object_id
        )


async def test_handling_multiple_coexisting_uploads(s3: S3Fixture):
    """
    Test that the invalid state of multiple uploads coexisting for the same object
    is correctly handled.
    """
    # initialize an upload:
    upload1_id, bucket_id, object_id = await s3.get_initialized_upload()

    # initialize another upload bypassing any checks:
    upload2_id = s3.storage._client.create_multipart_upload(
        Bucket=bucket_id, Key=object_id
    )["UploadId"]

    # try to work on both uploads:
    for upload_id in [upload1_id, upload2_id]:
        with pytest.raises(ObjectStorageProtocol.MultipleActiveUploadsError):
            await s3.storage.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=1,
            )

        with pytest.raises(ObjectStorageProtocol.MultipleActiveUploadsError):
            await s3.storage.complete_multipart_upload(
                upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
            )

    # aborting should work:
    await s3.storage.abort_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )

    # confirm that aborting one upload fixes the problem
    await upload_part(
        s3.storage,
        upload_id=upload1_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=b"Test content.",
        part_number=1,
    )
    await s3.storage.complete_multipart_upload(
        upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
    )


@pytest.mark.parametrize("abort_first", (True, False))
async def test_handling_multiple_subsequent_uploads(abort_first: bool, s3: S3Fixture):
    """
    Ensure that multiple subsequent uploads that target the same object are handled
    correctly. Three cases shall be distinguished:
        1. initiate an upload, upload some parts, abort it, then start another
          upload, uploads some parts, complete it (`abort_first` set to True)
        2. initiate an upload, upload some parts, complete it, then start another
          upload, uploads some parts, complete it (`abort_first` set to False)
    """
    # perform first upload:
    upload1_id, bucket_id, object_id = await s3.get_initialized_upload()

    async def upload_part_shortcut(upload_id):
        await upload_part(
            s3.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            content=b"Test content.",
            part_number=1,
        )

    await upload_part_shortcut(upload1_id)
    if abort_first:
        await s3.storage.abort_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )
    else:
        await s3.storage.complete_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )

    # perform second upload:
    upload2_id = await s3.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    await upload_part_shortcut(upload2_id)
    await s3.storage.complete_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )
