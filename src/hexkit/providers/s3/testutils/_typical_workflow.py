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
"""Contains a function for testing basic operations using an S3 service."""

# This workflow is defined as a separate function so that it can also be used
# outside of the `tests` package e.g. to test the compliance of an S3-compatible
# object storage implementation:
from pathlib import Path

from hexkit.protocols.objstorage import ObjectStorageProtocol

from ._utils import (
    download_and_check_test_file,
    multipart_upload_file,
    upload_file,
)


async def typical_workflow(  # noqa: PLR0913
    storage_client: ObjectStorageProtocol,
    test_file_path: Path,
    test_file_md5: str,
    bucket1_id: str = "example-bucket-1",
    bucket2_id: str = "excample-bucket-2",
    object_id: str = "example-object-1",
    use_multipart_upload: bool = True,
    part_size: int = ObjectStorageProtocol.DEFAULT_PART_SIZE,
):
    """Run a typical workflow of basic object operations using an S3 service."""
    print("Run a workflow for testing basic object operations using a S3 service:")

    print(f" - create new bucket {bucket1_id}")
    await storage_client.create_bucket(bucket1_id)

    print(" - confirm bucket creation")
    assert await storage_client.does_bucket_exist(bucket1_id)  # noqa: S101

    if use_multipart_upload:
        await multipart_upload_file(
            storage_dao=storage_client,
            bucket_id=bucket1_id,
            object_id=object_id,
            file_path=test_file_path,
            part_size=part_size,
        )
    else:
        print(f" - upload test object {object_id} to bucket")
        upload_url = await storage_client.get_object_upload_url(
            bucket_id=bucket1_id, object_id=object_id
        )
        upload_file(
            presigned_url=upload_url, file_path=test_file_path, file_md5=test_file_md5
        )

    print(" - confirm object upload")
    assert await storage_client.does_object_exist(  # noqa: S101
        bucket_id=bucket1_id, object_id=object_id
    )

    print(" - download and check object")
    download_url1 = await storage_client.get_object_download_url(
        bucket_id=bucket1_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url1, expected_md5=test_file_md5
    )

    print(f" - create a second bucket {bucket2_id} and move the object there")
    await storage_client.create_bucket(bucket2_id)
    await storage_client.copy_object(
        source_bucket_id=bucket1_id,
        source_object_id=object_id,
        dest_bucket_id=bucket2_id,
        dest_object_id=object_id,
    )
    await storage_client.delete_object(bucket_id=bucket1_id, object_id=object_id)

    print(" - confirm move")
    assert not await storage_client.does_object_exist(  # noqa: S101
        bucket_id=bucket1_id, object_id=object_id
    )
    assert await storage_client.does_object_exist(  # noqa: S101
        bucket_id=bucket2_id, object_id=object_id
    )

    print(f" - delete bucket {bucket1_id}")
    await storage_client.delete_bucket(bucket1_id)

    print(" - confirm bucket deletion")
    assert not await storage_client.does_bucket_exist(bucket1_id)  # noqa: S101

    print(f" - download object from bucket {bucket2_id}")
    download_url2 = await storage_client.get_object_download_url(
        bucket_id=bucket2_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url2, expected_md5=test_file_md5
    )

    print(f" - delete bucket {bucket2_id}")
    await storage_client.delete_object(bucket_id=bucket2_id, object_id=object_id)
    await storage_client.delete_bucket(bucket2_id)

    print(" - confirm bucket deletion")
    assert not await storage_client.does_bucket_exist(bucket2_id)  # noqa: S101

    print("Done.")
