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
#

"""Utilities for testing code that uses the S3ObjectStorage provider.

Please note, only use for testing purposes.
"""

import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, Optional

import pytest
import requests
from pydantic import BaseModel, validator
from testcontainers.localstack import LocalStackContainer

from hexkit.protocols.objstorage import ObjectStorageProtocol, PresignedPostURL
from hexkit.providers.s3.provider import S3ConfigBase, S3ObjectStorage

TEST_FILE_DIR = Path(__file__).parent.resolve() / "test_files"

TEST_FILE_PATHS = [
    TEST_FILE_DIR / filename
    for filename in os.listdir(TEST_FILE_DIR)
    if filename.startswith("test_") and filename.endswith(".yaml")
]

MEBIBYTE = 1024 * 1024


def calc_md5(content: bytes) -> str:
    """
    Calc the md5 checksum for the specified bytes.
    """
    return hashlib.md5(content).hexdigest()  # nosec


class ObjectFixture(BaseModel):
    """A Model for describing fixtures for the object storage."""

    file_path: Path
    bucket_id: str
    object_id: str
    content: bytes = b"will be overwritten"
    md5: str = "will be overwritten"

    # pylint: disable=no-self-argument,no-self-use
    @validator("content", always=True)
    def read_content(cls, _, values):
        """Read in the file content."""
        with open(values["file_path"], "rb") as file:
            return file.read()

    # pylint: disable=no-self-argument,no-self-use
    @validator("md5", always=True)
    def calc_md5_from_content(cls, _, values):
        """Calculate md5 based on the content."""
        return calc_md5(values["content"])


def upload_file(presigned_url: PresignedPostURL, file_path: Path, file_md5: str):
    """Uploads the test file to the specified URL"""
    with open(file_path, "rb") as test_file:
        files = {"file": (str(file_path), test_file)}
        headers = {"ContentMD5": file_md5}
        response = requests.post(
            presigned_url.url, data=presigned_url.fields, files=files, headers=headers
        )
        response.raise_for_status()


def check_part_size(file_path: Path, anticipated_size: int) -> None:
    """Check if the anticipated part size can be used to upload the specified file
    using the maximum number of file parts. Raises and exception otherwise."""
    file_size = os.path.getsize(file_path)
    if (file_size / anticipated_size) > ObjectStorageProtocol.MAX_FILE_PART_NUMBER:
        raise RuntimeError(
            f"The specified file ('{file_path}') cannot to be uploaded using the"
            + f" specified part size ({anticipated_size}') since the maximum number"
            + f" of parts ({ObjectStorageProtocol.MAX_FILE_PART_NUMBER}) would be"
            + "exceeded. Please choose a larger part size."
        )


# pylint: disable=too-many-arguments
async def upload_part(
    storage_dao: ObjectStorageProtocol,
    upload_id: str,
    bucket_id: str,
    object_id: str,
    content: bytes,
    part_number: int = 1,
):
    """Upload the specified content as part to an initialized multipart upload."""

    upload_url = await storage_dao.get_part_upload_url(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        part_number=part_number,
    )
    response = requests.put(upload_url, data=content)
    response.raise_for_status()


# pylint: disable=too-many-arguments
async def upload_part_of_size(
    storage_dao: ObjectStorageProtocol,
    upload_id: str,
    bucket_id: str,
    object_id: str,
    size: int,
    part_number: int,
):
    """
    Generate a bytes object of the specified size and uploads the part to an initialized
    multipart upload.
    """
    content = b"\0" * size
    await upload_part(
        storage_dao=storage_dao,
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=content,
        part_number=part_number,
    )


async def multipart_upload_file(
    storage_dao: ObjectStorageProtocol,
    bucket_id: str,
    object_id: str,
    file_path: Path,
    part_size: int = ObjectStorageProtocol.DEFAULT_PART_SIZE,
) -> None:
    """Uploads the test file to the specified URL"""

    check_part_size(file_path=file_path, anticipated_size=part_size)

    print(f" - initiate multipart upload for test object {object_id}")
    upload_id = await storage_dao.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    with open(file_path, "rb") as test_file:
        for part_number in range(1, ObjectStorageProtocol.MAX_FILE_PART_NUMBER + 1):
            print(f" - read {part_size} from file: {str(file_path)}")
            file_part = test_file.read(part_size)

            if not file_part:
                print(f" - everything uploaded with {part_number} parts")
                break

            print(f" - upload part number {part_number} using upload url")
            await upload_part(
                storage_dao=storage_dao,
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                content=file_part,
                part_number=part_number,
            )

    print(" - complete multipart upload")
    await storage_dao.complete_multipart_upload(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
    )


def download_and_check_test_file(presigned_url: str, expected_md5: str):
    """Downloads the test file from thespecified URL and checks its integrity (md5)."""

    response = requests.get(presigned_url)
    response.raise_for_status()

    observed_md5 = calc_md5(response.content)

    assert (  # nosec
        observed_md5 == expected_md5
    ), "downloaded file has unexpected md5 checksum"


DEFAULT_EXISTING_BUCKETS = [
    "myexistingtestbucket100",
    "myexistingtestbucket200",
]
DEFAULT_NON_EXISTING_BUCKETS = [
    "mynonexistingtestobject100",
    "mynonexistingtestobject200",
]

DEFAULT_EXISTING_OBJECTS = [
    ObjectFixture(
        file_path=file_path,
        bucket_id=f"myexistingtestbucket{idx}",
        object_id=f"myexistingtestobject{idx}",
    )
    for idx, file_path in enumerate(TEST_FILE_PATHS[0:2])
]

DEFAULT_NON_EXISTING_OBJECTS = [
    ObjectFixture(
        file_path=file_path,
        bucket_id=f"mynonexistingtestbucket{idx}",
        object_id=f"mynonexistingtestobject{idx}",
    )
    for idx, file_path in enumerate(TEST_FILE_PATHS[2:4])
]


async def populate_storage(
    storage: ObjectStorageProtocol,
    bucket_fixtures: List[str],
    object_fixtures: List[ObjectFixture],
):
    """Populate Storage with object and bucket fixtures"""

    for bucket_fixture in bucket_fixtures:
        await storage.create_bucket(bucket_fixture)

    for object_fixture in object_fixtures:
        if not await storage.does_bucket_exist(object_fixture.bucket_id):
            await storage.create_bucket(object_fixture.bucket_id)

        presigned_url = await storage.get_object_upload_url(
            bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
        )

        upload_file(
            presigned_url=presigned_url,
            file_path=object_fixture.file_path,
            file_md5=object_fixture.md5,
        )


def config_from_localstack_container(container: LocalStackContainer) -> S3ConfigBase:
    """Prepares a S3ConfigBase from an instance of a localstack test container."""
    s3_endpoint_url = container.get_url()
    return S3ConfigBase(  # nosec
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key_id="test",
        s3_secret_access_key="test",
    )


@dataclass
class S3Fixture:
    """Info yielded by the `s3_fixture` function"""

    config: S3ConfigBase
    storage: S3ObjectStorage
    existing_buckets: List[str]
    non_existing_buckets: List[str]
    existing_objects: List[ObjectFixture]
    non_existing_objects: List[ObjectFixture]


def s3_fixture_factory(
    existing_buckets: Optional[List[str]] = None,
    non_existing_buckets: Optional[List[str]] = None,
    existing_objects: Optional[List[ObjectFixture]] = None,
    non_existing_objects: Optional[List[ObjectFixture]] = None,
):
    """A factory for generating a pre-configured Pytest fixture working with S3."""

    # list defaults:
    # (listting instances of primitive types such as lists as defaults in the function
    # header is dangerous)
    existing_buckets_ = (
        DEFAULT_EXISTING_BUCKETS if existing_buckets is None else existing_buckets
    )
    non_existing_buckets_ = (
        DEFAULT_NON_EXISTING_BUCKETS
        if non_existing_buckets is None
        else non_existing_buckets
    )
    existing_objects_ = (
        DEFAULT_EXISTING_OBJECTS if existing_objects is None else existing_objects
    )
    non_existing_objects_ = (
        DEFAULT_NON_EXISTING_OBJECTS
        if non_existing_objects is None
        else non_existing_objects
    )

    @pytest.fixture
    async def s3_fixture():
        """Pytest fixture for tests depending on the S3ObjectStorage DAO."""
        with LocalStackContainer(image="localstack/localstack:0.14.2").with_services(
            "s3"
        ) as localstack:
            config = config_from_localstack_container(localstack)

            storage = S3ObjectStorage(config=config)
            await populate_storage(
                storage=storage,
                bucket_fixtures=existing_buckets_,
                object_fixtures=existing_objects_,
            )

            assert not set(existing_buckets_) & set(  # nosec
                non_existing_buckets_
            ), "The existing and non existing bucket lists may not overlap"

            yield S3Fixture(
                config=config,
                storage=storage,
                existing_buckets=existing_buckets_,
                non_existing_buckets=non_existing_buckets_,
                existing_objects=existing_objects_,
                non_existing_objects=non_existing_objects_,
            )

    return s3_fixture


# This workflow is defined as a seperate function so that it can also be used
# outside of the `tests` package:
# pylint: disable=too-many-arguments
async def typical_workflow(
    storage_client: ObjectStorageProtocol,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = DEFAULT_NON_EXISTING_OBJECTS[0].object_id,
    test_file_path: Path = DEFAULT_NON_EXISTING_OBJECTS[0].file_path,
    test_file_md5: str = DEFAULT_NON_EXISTING_OBJECTS[0].md5,
    use_multipart_upload: bool = True,
    part_size: int = ObjectStorageProtocol.DEFAULT_PART_SIZE,
):
    """
    Run a typical workflow of basic object operations using a S3 service.
    """
    print("Run a workflow for testing basic object operations using a S3 service:")

    print(f" - create new bucket {bucket1_id}")
    await storage_client.create_bucket(bucket1_id)

    print(" - confirm bucket creation")
    assert await storage_client.does_bucket_exist(bucket1_id)  # nosec

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
    assert await storage_client.does_object_exist(  # nosec
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
    assert not await storage_client.does_object_exist(  # nosec
        bucket_id=bucket1_id, object_id=object_id
    )
    assert await storage_client.does_object_exist(  # nosec
        bucket_id=bucket2_id, object_id=object_id
    )

    print(f" - delete bucket {bucket1_id}")
    await storage_client.delete_bucket(bucket1_id)

    print(" - confirm bucket deletion")
    assert not await storage_client.does_bucket_exist(bucket1_id)  # nosec

    print(f" - download object from bucket {bucket2_id}")
    download_url2 = await storage_client.get_object_download_url(
        bucket_id=bucket2_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url2, expected_md5=test_file_md5
    )

    print("Done.")


async def get_initialized_upload(s3_fixture: S3Fixture):
    """Initialize a new empty multipart upload."""

    bucket_id = s3_fixture.existing_buckets[0]
    object_id = s3_fixture.non_existing_objects[0].object_id
    upload_id = await s3_fixture.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    return upload_id, bucket_id, object_id


async def prepare_non_completed_upload(s3_fixture: S3Fixture):
    """Prepare an upload that has not been marked as completed, yet."""

    upload_id, bucket_id, object_id = await get_initialized_upload(s3_fixture)

    object_fixture = s3_fixture.non_existing_objects[0]

    upload_part(
        storage_dao=s3_fixture.storage,
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=object_fixture.content,
    )

    return upload_id, bucket_id, object_id


@contextmanager
def big_temp_file(size: int):
    """Generates a big file with approximately the specified size in bytes."""
    current_size = 0
    current_number = 0
    next_number = 1
    with NamedTemporaryFile("w+b") as temp_file:
        while current_size <= size:
            byte_addition = f"{current_number}\n".encode("ASCII")
            current_size += len(byte_addition)
            temp_file.write(byte_addition)
            previous_number = current_number
            current_number = next_number
            next_number = previous_number + current_number
        temp_file.flush()
        yield temp_file
