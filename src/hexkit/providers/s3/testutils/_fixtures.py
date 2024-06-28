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
#

"""Utilities for testing code that uses the S3ObjectStorage provider.

Please note, only use for testing purposes.
"""

import os
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, NamedTuple, Optional

try:
    from typing import Self
except ImportError:  # Python < 3.11
    from typing_extensions import Self

import pytest
import pytest_asyncio
from pydantic import SecretStr
from testcontainers.localstack import LocalStackContainer

from hexkit.custom_types import PytestScope
from hexkit.providers.s3.provider import S3Config, S3ObjectStorage

from ._utils import FileObject, calc_md5, populate_storage, upload_file, upload_part

__all__ = [
    "LOCALSTACK_IMAGE",
    "MEBIBYTE",
    "TEST_FILE_DIR",
    "TEST_FILE_PATHS",
    "upload_file",
    "populate_storage",
    "calc_md5",
    "FileObject",
    "S3ContainerFixture",
    "get_s3_container_fixture",
    "s3_container_fixture",
    "S3Fixture",
    "get_persistent_s3_fixture",
    "persistent_s3_fixture",
    "get_clean_s3_fixture",
    "clean_s3_fixture",
    "s3_fixture",
    "tmp_file",
    "temp_file_object",
]


LOCALSTACK_IMAGE = "localstack/localstack:3.5.0"

TEST_FILE_DIR = Path(__file__).parent.parent.resolve() / "test_files"

TEST_FILE_PATHS = [
    TEST_FILE_DIR / filename
    for filename in os.listdir(TEST_FILE_DIR)
    if filename.startswith("test_") and filename.endswith(".yaml")
]

MEBIBYTE = 1024 * 1024


class UploadDetails(NamedTuple):
    """Encapsulates the details of an S3 upload process."""

    upload_id: str
    bucket_id: str
    object_id: str


class S3Fixture:
    """A fixture with utility methods for tests that use S3 file storage"""

    def __init__(self, config: S3Config, storage: S3ObjectStorage):
        """Initialize with config."""
        self.config = config
        self.storage = storage

    def get_buckets(self) -> set[str]:
        """Return a list of the buckets currently existing in the S3 object storage."""
        response = self.storage._client.list_buckets()
        buckets = {bucket["Name"] for bucket in response["Buckets"]}
        return buckets

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

    async def empty_buckets(self, buckets_to_exclude: Optional[list[str]] = None):
        """Clean the test artifacts or files from the populated buckets."""
        for bucket in self.get_buckets().difference(buckets_to_exclude or []):
            # Get list of all objects in the bucket
            object_ids = await self.storage.list_all_object_ids(bucket_id=bucket)
            # Delete all of these objects
            for object_id in object_ids:
                await self.storage.delete_object(bucket_id=bucket, object_id=object_id)

    async def delete_buckets(self, buckets_to_exclude: Optional[list[str]] = None):
        """Delete the populated buckets."""
        for bucket in self.get_buckets().difference(buckets_to_exclude or []):
            await self.storage.delete_bucket(bucket, delete_content=True)

    async def get_initialized_upload(self) -> UploadDetails:
        """Initialize a new empty multipart upload process.

        This does not upload any parts yet, only initializes the process.
        Returns the upload ID, bucket ID, and object ID.
        """
        bucket_id = "mybucketwithupload001"
        object_id = "myobjecttobeuploaded001"

        await self.populate_buckets([bucket_id])

        upload_id = await self.storage.init_multipart_upload(
            bucket_id=bucket_id, object_id=object_id
        )

        upload_details = UploadDetails(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

        return upload_details

    async def prepare_non_completed_upload(
        self,
    ) -> UploadDetails:
        """Prepare an upload that has not been marked as completed, yet.

        This calls `get_initialized_upload` to create the upload expectation,
        then uploads content to the bucket.

        Returns the upload ID, bucket ID, and object ID of the upload.
        """
        upload_details = await self.get_initialized_upload()

        with temp_file_object() as file:
            await upload_part(
                storage_dao=self.storage,
                upload_id=upload_details.upload_id,
                bucket_id=upload_details.bucket_id,
                object_id=upload_details.object_id,
                content=file.content,
            )

        return upload_details


class S3ContainerFixture(LocalStackContainer):
    """LocalStack test container with S3 configuration."""

    s3_config: S3Config

    def __init__(
        self,
        port: int = 4566,
        region_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the container."""
        super().__init__(image=LOCALSTACK_IMAGE)

    def __enter__(self) -> Self:
        """Enter the container context."""
        super().__enter__()
        s3_endpoint_url = self.get_url()
        s3_config = S3Config(  # type: ignore [call-arg]
            s3_endpoint_url=s3_endpoint_url,
            s3_access_key_id="test",
            s3_secret_access_key=SecretStr("test"),
        )
        self.s3_config = s3_config
        return self


def _s3_container_fixture() -> Generator[S3ContainerFixture, None, None]:
    """Fixture function for getting a running S3 test container."""
    with S3ContainerFixture() as s3_container:
        yield s3_container


def get_s3_container_fixture(
    scope: PytestScope = "session", name: str = "s3_container"
):
    """Get a LocalStack test container fixture with desired scope and name.

    By default, the session scope is used for LocalStack test containers.
    """
    return pytest.fixture(_s3_container_fixture, scope=scope, name=name)


s3_container_fixture = get_s3_container_fixture()


def _persistent_s3_fixture(
    s3_container: S3ContainerFixture,
) -> Generator[S3Fixture, None, None]:
    """Fixture function that gets a persistent S3 storage fixture.

    The state of the S3 storage is not cleaned up by the function.
    """
    config = s3_container.s3_config
    storage = S3ObjectStorage(config=config)

    yield S3Fixture(config=config, storage=storage)


def get_persistent_s3_fixture(scope: PytestScope = "function", name: str = "s3"):
    """Get an S3 fixture with desired scope and name.

    The state of the LocalStack test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying LocalStack test container.
    """
    return pytest.fixture(_persistent_s3_fixture, scope=scope, name=name)


persistent_s3_fixture = get_persistent_s3_fixture()


async def _clean_s3_fixture(
    s3_container: S3ContainerFixture,
) -> AsyncGenerator[S3Fixture, None]:
    """Async fixture function that gets a clean S3 storage fixture.

    The clean state is achieved by deleting all S3 buckets upfront.
    """
    for s3_fixture in _persistent_s3_fixture(s3_container):
        await s3_fixture.delete_buckets()
        yield s3_fixture


def get_clean_s3_fixture(scope: PytestScope = "function", name: str = "s3"):
    """Get an S3 storage fixture with desired scope and name.

    The state of the S3 storage is reset by deleting all buckets before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying LocalStack test container.
    """
    return pytest_asyncio.fixture(_clean_s3_fixture, scope=scope, name=name)


s3_fixture = clean_s3_fixture = get_clean_s3_fixture()


@contextmanager
def temp_file_object(
    bucket_id: str = "default-test-bucket",
    object_id: str = "default-test-object",
    size: int = 5 * MEBIBYTE,
) -> Generator[FileObject, None, None]:
    """Generate a file object with the specified size in bytes."""
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
            file_path=Path(temp_file.name), bucket_id=bucket_id, object_id=object_id
        )


@pytest.fixture()
def tmp_file() -> Generator[FileObject, None, None]:
    """A fixture that provides a temporary file."""
    with temp_file_object() as temp_file:
        yield temp_file
