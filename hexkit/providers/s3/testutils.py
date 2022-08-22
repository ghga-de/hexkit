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
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Generator, List

import pytest
import pytest_asyncio
import requests
from pydantic import BaseModel, validator
from testcontainers.localstack import LocalStackContainer

from hexkit.protocols.objstorage import ObjectStorageProtocol, PresignedPostURL
from hexkit.providers.s3.provider import S3Config, S3ObjectStorage

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


class FileObject(BaseModel):
    """A Model for describing objects in an object storage."""

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


class S3Fixture:
    """Yielded by the `s3_fixture` function"""

    def __init__(self, config: S3Config, storage: S3ObjectStorage):
        """Initialize with config."""
        self.config = config
        self.storage = storage

    async def populate_buckets(self, buckets: list[str]):
        """Populate the storage with buckets."""

        await populate_storage(
            self.storage, bucket_fixtures=buckets, object_fixtures=[]
        )

    async def populate_file_objects(self, file_objects: list[FileObject]):
        """Populate the storage with file objects."""

        await populate_storage(
            self.storage, bucket_fixtures=[], object_fixtures=file_objects
        )


@pytest_asyncio.fixture
def s3_fixture() -> Generator[S3Fixture, None, None]:
    """Pytest fixture for tests depending on the S3ObjectStorage DAO."""

    with LocalStackContainer(image="localstack/localstack:0.14.2").with_services(
        "s3"
    ) as localstack:
        config = config_from_localstack_container(localstack)

        storage = S3ObjectStorage(config=config)
        yield S3Fixture(config=config, storage=storage)


@contextmanager
def temp_file_object(
    bucket_id: str = "mydefaulttestbucket001",
    object_id: str = "mydefaulttestobject001",
    size: int = 5 * MEBIBYTE,
) -> Generator[FileObject, None, None]:
    """Generates a file object with the specified size in bytes."""

    chunk_size = 1024
    chunk = b"\0" * chunk_size
    current_size = 0
    with NamedTemporaryFile("w+b") as temp_file:
        while True:
            if current_size + chunk_size >= size:
                temp_file.write(chunk[: size - current_size])
                break
            temp_file.write(chunk)
            current_size += chunk_size
        temp_file.flush()

        yield FileObject(
            file_path=temp_file.name, bucket_id=bucket_id, object_id=object_id
        )


@pytest.fixture
def file_fixture():
    """A fixture that provides a temporary file."""

    with temp_file_object() as temp_file:
        yield temp_file


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
    using the maximum number of file parts. Raises an exception otherwise."""

    file_size = os.path.getsize(file_path)
    if file_size / anticipated_size > ObjectStorageProtocol.MAX_FILE_PART_NUMBER:
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


def upload_part_via_url(*, url: str, size: int):
    """Upload a file part of given size using the given URL."""

    content = b"\0" * size
    response = requests.put(url, data=content)
    response.raise_for_status()


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
    """Download the test file from the specified URL and check its integrity (md5)."""

    response = requests.get(presigned_url)
    response.raise_for_status()

    observed_md5 = calc_md5(response.content)

    assert (  # nosec
        observed_md5 == expected_md5
    ), "downloaded file has unexpected md5 checksum"


async def populate_storage(
    storage: ObjectStorageProtocol,
    bucket_fixtures: List[str],
    object_fixtures: List[FileObject],
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


def config_from_localstack_container(container: LocalStackContainer) -> S3Config:
    """Prepares a S3Config from an instance of a localstack test container."""

    s3_endpoint_url = container.get_url()
    return S3Config(  # nosec
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key_id="test",
        s3_secret_access_key="test",
    )


async def get_initialized_upload(s3_fixture_: S3Fixture):
    """Initialize a new empty multipart upload."""

    bucket_id = "mybucketwithupload001"
    object_id = "myobjecttobeuploaded001"

    await s3_fixture_.populate_buckets([bucket_id])

    upload_id = await s3_fixture_.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    return upload_id, bucket_id, object_id


async def prepare_non_completed_upload(s3_fixture_: S3Fixture):
    """Prepare an upload that has not been marked as completed, yet."""

    upload_id, bucket_id, object_id = await get_initialized_upload(s3_fixture_)

    with temp_file_object() as file:
        await upload_part(
            storage_dao=s3_fixture_.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            content=file.content,
        )

    return upload_id, bucket_id, object_id


# This workflow is defined as a seperate function so that it can also be used
# outside of the `tests` package e.g. to test the compliance of an S3-compatible
# object storage implementation:
# pylint: disable=too-many-arguments
async def typical_workflow(
    storage_client: ObjectStorageProtocol,
    test_file_path: Path,
    test_file_md5: str,
    bucket1_id: str = "mytestbucket001",
    bucket2_id: str = "mytestbucket002",
    object_id: str = "mytestobject001",
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
