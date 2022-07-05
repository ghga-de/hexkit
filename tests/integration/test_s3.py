# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

"""
Test S3 storage DAO
"""

from typing import ContextManager, Optional

import pytest
from black import nullcontext

from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import (
    MEBIBYTE,
    FileObject,
    S3Fixture,
    get_initialized_upload,
    prepare_non_completed_upload,
    temp_file_object,
    typical_workflow,
    upload_part,
    upload_part_of_size,
)

EXAMPLE_BUCKETS = [
    "mytestbucket100",
    "mytestbucket200",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_multipart_upload", [True, False])
async def test_typical_workflow(
    use_multipart_upload: bool,
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow.
    """

    with temp_file_object(size=20 * MEBIBYTE) as file:
        await typical_workflow(
            storage_client=s3_fixture.storage,
            bucket1_id=EXAMPLE_BUCKETS[0],
            bucket2_id=EXAMPLE_BUCKETS[1],
            object_id=file.object_id,
            test_file_md5=file.md5,
            test_file_path=file.file_path,
            use_multipart_upload=use_multipart_upload,
        )


@pytest.mark.asyncio
async def test_object_existence_checks(
    s3_fixture: S3Fixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Test if the checks for existence of objects work correctly."""

    # object should not exist in the beginning:
    assert not await s3_fixture.storage.does_object_exist(
        bucket_id=file_fixture.bucket_id, object_id=file_fixture.object_id
    )

    # add the corresponding file object to the storage:
    await s3_fixture.populate_file_objects([file_fixture])

    # now the object should exist:
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=file_fixture.bucket_id, object_id=file_fixture.object_id
    )


@pytest.mark.asyncio
async def test_bucket_existence_checks(s3_fixture: S3Fixture):  # noqa: F811
    """Test if the checks for existence of buckets work correctly."""

    bucket_id = EXAMPLE_BUCKETS[0]

    # bucket should not exist in the beginning:
    assert not await s3_fixture.storage.does_bucket_exist(bucket_id=bucket_id)

    # add the corresponding bucket to the storage:
    await s3_fixture.populate_buckets([bucket_id])

    # now the bucket should exist:
    assert await s3_fixture.storage.does_bucket_exist(bucket_id=bucket_id)


@pytest.mark.asyncio
async def test_object_and_bucket_collisions(
    s3_fixture: S3Fixture, file_fixture: FileObject  # noqa: F811
):
    """
    Tests whether overwriting (re-creation, re-upload, or copy to existing object)
    fails with the expected error.
    """

    await s3_fixture.populate_file_objects([file_fixture])

    with pytest.raises(ObjectStorageProtocol.BucketAlreadyExistsError):
        await s3_fixture.storage.create_bucket(file_fixture.bucket_id)

    with pytest.raises(ObjectStorageProtocol.BucketNotEmptyError):
        await s3_fixture.storage.delete_bucket(file_fixture.bucket_id)

    with pytest.raises(ObjectStorageProtocol.ObjectAlreadyExistsError):
        await s3_fixture.storage.get_object_upload_url(
            bucket_id=file_fixture.bucket_id, object_id=file_fixture.object_id
        )

    with pytest.raises(ObjectStorageProtocol.ObjectAlreadyExistsError):
        await s3_fixture.storage.copy_object(
            source_bucket_id=file_fixture.bucket_id,
            source_object_id=file_fixture.object_id,
            dest_bucket_id=file_fixture.bucket_id,
            dest_object_id=file_fixture.object_id,
        )


@pytest.mark.asyncio
async def test_handling_non_existing_file_and_bucket(
    s3_fixture: S3Fixture, file_fixture: FileObject  # noqa: F811
):
    """
    Tests whether interacting with a non-existing bucket/file object fails with the
    expected result.
    """

    non_existing_bucket_id = "mynonexistingbucket001"
    non_existing_object_id = "mynonexistingobject001"
    existing_bucket_id = file_fixture.bucket_id
    existing_object_id = file_fixture.object_id

    await s3_fixture.populate_file_objects([file_fixture])

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3_fixture.storage.delete_bucket(non_existing_bucket_id)

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3_fixture.storage.get_object_download_url(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3_fixture.storage.get_object_upload_url(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        await s3_fixture.storage.delete_object(
            bucket_id=non_existing_bucket_id,
            object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        # copy when source does not exist:
        await s3_fixture.storage.copy_object(
            source_bucket_id=non_existing_bucket_id,
            source_object_id=non_existing_object_id,
            dest_bucket_id=existing_bucket_id,
            dest_object_id=existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.BucketNotFoundError):
        # copy when source does not exist:
        await s3_fixture.storage.copy_object(
            source_bucket_id=existing_bucket_id,
            source_object_id=existing_object_id,
            dest_bucket_id=non_existing_bucket_id,
            dest_object_id=non_existing_object_id,
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        await s3_fixture.storage.get_object_download_url(
            bucket_id=existing_bucket_id, object_id=non_existing_object_id
        )

    with pytest.raises(ObjectStorageProtocol.ObjectNotFoundError):
        await s3_fixture.storage.delete_object(
            bucket_id=existing_bucket_id, object_id=non_existing_object_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_content", (True, False))
async def test_delete_non_empty_bucket(
    delete_content: bool,
    s3_fixture: S3Fixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Test deleting an non-empty bucket."""

    await s3_fixture.populate_file_objects([file_fixture])

    with nullcontext() if delete_content else pytest.raises(  # type: ignore
        ObjectStorageProtocol.BucketNotEmptyError
    ):
        await s3_fixture.storage.delete_bucket(
            bucket_id=file_fixture.bucket_id, delete_content=delete_content
        )


@pytest.mark.asyncio
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
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Makes sure that using a non existing upload_id-bucket_id-object_id combination
    throws the right error.
    """

    # prepare a non-completed upload:
    real_upload_id, real_bucket_id, real_object_id = await prepare_non_completed_upload(
        s3_fixture
    )

    upload_id = real_upload_id if upload_id_correct else "wrong-upload"
    bucket_id = real_bucket_id if bucket_id_correct else "wrong-bucket"
    object_id = real_object_id if object_id_correct else "wrong-object"

    def get_exception_context() -> ContextManager:
        return pytest.raises(exception) if exception else nullcontext()  # type: ignore

    # call relevant methods from the provider:
    with pytest.raises(exception):
        await s3_fixture.storage._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

    with pytest.raises(exception):
        await s3_fixture.storage.get_part_upload_url(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id, part_number=1
        )

    with pytest.raises(exception):
        await s3_fixture.storage.complete_multipart_upload(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "part_number, exception",
    [(0, ValueError), (1, None), (10000, None), (10001, ValueError)],
)
async def test_invalid_part_number(
    part_number: int,
    exception: Optional[Exception],
    s3_fixture: S3Fixture,  # noqa: F811
):
    """Check that invalid part numbers are cached correcly."""

    upload_id, bucket_id, object_id = await prepare_non_completed_upload(s3_fixture)

    with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
        _ = await s3_fixture.storage.get_part_upload_url(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            part_number=part_number,
        )


@pytest.mark.asyncio
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
        ),  # Missmatch with anticipated parts
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
        ),  # missmatch anticipated part size
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
    exception: Optional[Exception],
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Test the complete_multipart_upload method.
    """

    upload_id, bucket_id, object_id = await get_initialized_upload(s3_fixture)
    for part_idx, part_size in enumerate(part_sizes):
        await upload_part_of_size(
            storage_dao=s3_fixture.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            size=part_size,
            part_number=part_idx + 1,
        )

    with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
        await s3_fixture.storage.complete_multipart_upload(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            anticipated_part_quantity=anticipated_part_quantity,
            anticipated_part_size=anticipated_part_size,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_upload", (True, False))
async def test_abort_multipart_upload(
    empty_upload: bool,
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Test the abort_multipart_upload method.
    """

    upload_id, bucket_id, object_id = await get_initialized_upload(s3_fixture)

    async def upload_part_shortcut(part_number):
        await upload_part_of_size(
            storage_dao=s3_fixture.storage,
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

    await s3_fixture.storage.abort_multipart_upload(
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


@pytest.mark.asyncio
async def test_multiple_active_uploads(s3_fixture: S3Fixture):  # noqa: F811
    """
    Test that multiple active uploads for the same object are not possible.
    """

    # initialize an upload:
    _, bucket_id, object_id = await get_initialized_upload(s3_fixture)

    # initialize another upload for the same object:
    with pytest.raises(ObjectStorageProtocol.MultiPartUploadAlreadyExistsError):
        _ = await s3_fixture.storage.init_multipart_upload(
            bucket_id=bucket_id, object_id=object_id
        )


@pytest.mark.asyncio
async def test_handling_multiple_coexisting_uploads(
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Test that the invalid state of multiple uploads coexisting for the same object
    is correctly handeled.
    """

    # initialize an upload:
    upload1_id, bucket_id, object_id = await get_initialized_upload(s3_fixture)

    # initialize another upload bypassing any checks:
    upload2_id = s3_fixture.storage._client.create_multipart_upload(
        Bucket=bucket_id, Key=object_id
    )["UploadId"]

    # try to work on both uploads:
    for upload_id in [upload1_id, upload2_id]:
        with pytest.raises(ObjectStorageProtocol.MultipleActiveUploadsError):
            await s3_fixture.storage.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=1,
            )

        with pytest.raises(ObjectStorageProtocol.MultipleActiveUploadsError):
            await s3_fixture.storage.complete_multipart_upload(
                upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
            )

    # aborting should work:
    await s3_fixture.storage.abort_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )

    # confirm that aborting one upload fixes the problem
    await upload_part(
        s3_fixture.storage,
        upload_id=upload1_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=b"Test content.",
        part_number=1,
    )
    await s3_fixture.storage.complete_multipart_upload(
        upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("abort_first", (True, False))
async def test_handling_multiple_subsequent_uploads(
    abort_first: bool, s3_fixture: S3Fixture  # noqa: F811
):
    """
    Ensure that multiple subsequent uploads that target the same object are handled
    correctly. Three cases shall be distinguished:
        1. initiate an upload, upload some parts, abort it, then start another
          upload, uploads some parts, complete it (`abort_first` set to True)
        2. initiate an upload, upload some parts, complete it, then start another
          upload, uploads some parts, complete it (`abort_first` set to False)
    """

    # perform first upload:
    upload1_id, bucket_id, object_id = await get_initialized_upload(s3_fixture)

    async def upload_part_shortcut(upload_id):
        await upload_part(
            s3_fixture.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            content=b"Test content.",
            part_number=1,
        )

    await upload_part_shortcut(upload1_id)
    if abort_first:
        await s3_fixture.storage.abort_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )
    else:
        await s3_fixture.storage.complete_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )

    # perform second upload:
    upload2_id = await s3_fixture.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    await upload_part_shortcut(upload2_id)
    await s3_fixture.storage.complete_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )
