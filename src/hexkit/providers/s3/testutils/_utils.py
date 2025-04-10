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
"""Functionality to support S3-related testing"""

# ruff: noqa: PLR0913

import hashlib
import os
from pathlib import Path

import requests
from pydantic import BaseModel, computed_field

from hexkit.protocols.objstorage import ObjectStorageProtocol, PresignedPostURL

__all__ = [
    "check_part_size",
    "download_and_check_test_file",
    "multipart_upload_file",
    "upload_part",
    "upload_part_of_size",
    "upload_part_via_url",
]

TIMEOUT = 60


class FileObject(BaseModel):
    """A Model for describing objects in an object storage."""

    file_path: Path
    bucket_id: str
    object_id: str

    @computed_field  # type: ignore [prop-decorator]
    @property
    def content(self) -> bytes:
        """Extract the content from the file at the provided path"""
        if not self.file_path:
            raise ValueError("`FileObject.file_path` must not be empty.")
        with open(self.file_path, "rb") as file:
            return file.read()

    @computed_field  # type: ignore [prop-decorator]
    @property
    def md5(self) -> str:
        """Calculate the md5 hash of the content"""
        return calc_md5(self.content)


def calc_md5(content: bytes) -> str:
    """Calc the md5 checksum for the specified bytes."""
    return hashlib.md5(content, usedforsecurity=False).hexdigest()  # nosec


def check_part_size(file_path: Path, anticipated_size: int) -> None:
    """Check if the anticipated part size can be used to upload the specified file
    using the maximum number of file parts. Raises an exception otherwise.
    """
    file_size = os.path.getsize(file_path)
    if file_size / anticipated_size > ObjectStorageProtocol.MAX_FILE_PART_NUMBER:
        raise RuntimeError(
            f"The specified file ('{file_path}') cannot to be uploaded using the"
            + f" specified part size ({anticipated_size}') since the maximum number"
            + f" of parts ({ObjectStorageProtocol.MAX_FILE_PART_NUMBER}) would be"
            + "exceeded. Please choose a larger part size."
        )


async def populate_storage(
    storage: ObjectStorageProtocol,
    bucket_fixtures: list[str],
    object_fixtures: list[FileObject],
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


def upload_file(presigned_url: PresignedPostURL, file_path: Path, file_md5: str):
    """Uploads the test file to the specified URL"""
    with open(file_path, "rb") as test_file:
        files = {"file": (str(file_path), test_file)}
        headers = {"ContentMD5": file_md5}
        response = requests.post(
            presigned_url.url,
            data=presigned_url.fields,
            files=files,
            headers=headers,
            timeout=TIMEOUT,
        )
        response.raise_for_status()


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
    response = requests.put(upload_url, data=content, timeout=TIMEOUT)
    response.raise_for_status()


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
    response = requests.put(url, data=content, timeout=TIMEOUT)
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
    response = requests.get(presigned_url, timeout=TIMEOUT)
    response.raise_for_status()

    observed_md5 = calc_md5(response.content)

    assert (  # noqa: S101
        observed_md5 == expected_md5
    ), "downloaded file has unexpected md5 checksum"
