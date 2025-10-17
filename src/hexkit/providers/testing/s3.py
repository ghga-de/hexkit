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

"""Mock object storage class"""

from collections import defaultdict
from uuid import uuid4

from hexkit.protocols.objstorage import ObjectStorageProtocol, PresignedPostURL
from hexkit.providers.s3 import S3Config

UploadID = str
BucketID = str
ObjectID = str


class InMemObjectStorage(ObjectStorageProtocol):
    """In-memory object storage mock just the method used in this service"""

    def __init__(self, *, config: S3Config):
        """Set bucket ID.

        Type aliases are used here only to clarify dictionary structure.
        """
        self.buckets: dict[BucketID, set[ObjectID]] = defaultdict(set)
        self.uploads: dict[BucketID, dict[ObjectID, set[UploadID]]] = defaultdict(dict)
        self.endpoint_url = config.s3_endpoint_url

    async def _does_bucket_exist(self, bucket_id: str) -> bool:
        """Check whether a bucket with the specified ID (`bucket_id`) exists.

        Returns `True` if it exists and `False` otherwise.
        """
        return bucket_id in self.buckets

    async def _assert_bucket_exists(self, bucket_id: str):
        """Assert that the bucket with the specified ID (`bucket_id`) exists.

        Raises a `BucketNotFoundError` if the bucket does not exist.
        """
        if not await self.does_bucket_exist(bucket_id):
            raise self.BucketNotFoundError(bucket_id=bucket_id)

    async def _create_bucket(self, bucket_id: str) -> None:
        """Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.

        Raises a `BucketAlreadyExistsError` if the bucket already exists.
        """
        if bucket_id in self.buckets:
            raise self.BucketAlreadyExistsError(bucket_id=bucket_id)
        self.buckets[bucket_id] = set()

    async def _delete_bucket(
        self, bucket_id: str, *, delete_content: bool = False
    ) -> None:
        """
        Delete a bucket with the specified unique ID.

        Raises a `BucketNotEmptyError` if the bucket is not empty and `delete_content`
        is set to False.
        """
        await self._assert_bucket_exists(bucket_id)
        if self.buckets[bucket_id] and not delete_content:
            raise self.BucketNotEmptyError(bucket_id)
        del self.buckets[bucket_id]

    async def _list_all_object_ids(self, *, bucket_id: str) -> list[str]:
        """Retrieve a list of IDs for all objects currently present in the specified bucket.

        Raises a `BucketNotFoundError` if the bucket does not exist.
        """
        await self._assert_bucket_exists(bucket_id)
        return sorted(self.buckets[bucket_id])

    async def _does_object_exist(
        self, *, bucket_id: str, object_id: str, object_md5sum: str | None = None
    ) -> bool:
        """Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Return `True` if checks succeed and `False` otherwise.
        """
        if object_md5sum is not None:
            raise NotImplementedError("MD5 checking is not yet implemented.")
        return object_id in self.buckets[bucket_id]

    async def _assert_object_exists(self, *, bucket_id: str, object_id: str) -> None:
        """Asserts that the file with specified ID exists in the given bucket.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectNotFoundError`: If the object does not exist in the bucket.
        """
        # first check if bucket exists:
        await self._assert_bucket_exists(bucket_id)

        if not await self.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            raise self.ObjectNotFoundError(bucket_id=bucket_id, object_id=object_id)

    async def _assert_object_not_exists(
        self, *, bucket_id: str, object_id: str
    ) -> None:
        """Asserts that the file with specified ID does NOT exist in the given bucket.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectAlreadyExistsError`: If the object exists in the bucket.
        """
        # first check if bucket exists:
        await self._assert_bucket_exists(bucket_id)

        if await self.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            raise self.ObjectAlreadyExistsError(
                bucket_id=bucket_id, object_id=object_id
            )

    async def _get_object_upload_url(
        self,
        *,
        bucket_id: str,
        object_id: str,
        expires_after: int = 86400,
        max_upload_size: int | None = None,
    ) -> PresignedPostURL:
        """Generates and returns a FAKE HTTP URL to upload a new file object with the
        given id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`) and
        a maximum size (bytes) for uploads (`max_upload_size`).

        This URL does not point to an actual resource or API, it is merely for testing.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectAlreadyExistsError`: If the object exists in the bucket.
        """
        await self._assert_object_not_exists(bucket_id=bucket_id, object_id=object_id)
        fields: dict[str, str] = {"expires_after": str(expires_after)}
        if max_upload_size:
            fields["max_upload_size"] = str(max_upload_size)
        return PresignedPostURL(
            url=f"https://s3.example.com/{bucket_id}/{object_id}", fields=fields
        )

    async def _list_multipart_upload_for_object(
        self, *, bucket_id: str, object_id: str
    ) -> list[str]:
        """Lists all active multipart uploads for the given object. Returns a list of
        their IDs.

        (S3 allows multiple ongoing multi-part uploads.)

        Raises a `BucketNotFoundError` if the bucket does not exist.
        """
        await self._assert_bucket_exists(bucket_id)
        uploads_for_object = self.uploads[bucket_id].get(object_id, set())
        return sorted(uploads_for_object)

    async def _assert_no_multipart_upload(self, *, bucket_id: str, object_id: str):
        """Ensure that there are no active multi-part uploads for the given object.

        Raises:
            `MultiPartUploadAlreadyExistsError`: If an upload exists for the object.
            `BucketNotFoundError`: If the bucket does not exist.
        """
        upload_ids = await self._list_multipart_upload_for_object(
            bucket_id=bucket_id, object_id=object_id
        )
        if upload_ids:
            raise self.MultiPartUploadAlreadyExistsError(
                bucket_id=bucket_id, object_id=object_id
            )

    async def _assert_multipart_upload_exists(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        assert_exclusiveness: bool = True,
    ) -> None:
        """Checks if a multipart upload with the given ID exists and whether it maps
        to the specified object and bucket.

        By default, also verifies that this upload is the only upload active for
        that file.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `MultiPartUploadNotFoundError`: If the upload ID doesn't exist.
            `MultipleActiveUploadsError`: If `assert_exclusiveness` is True and there are
                multiple open uploads for the object ID.
        """
        upload_ids = await self._list_multipart_upload_for_object(
            bucket_id=bucket_id, object_id=object_id
        )

        if assert_exclusiveness and len(upload_ids) > 1:
            raise self.MultipleActiveUploadsError(
                bucket_id=bucket_id, object_id=object_id, upload_ids=upload_ids
            )

        if upload_id not in upload_ids:
            raise self.MultiPartUploadNotFoundError(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            )

    async def _delete_object(self, *, bucket_id: str, object_id: str) -> None:
        """Delete an object with the specified id (`object_id`) in the bucket with the
        specified id (`bucket_id`).

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectNotFoundError`: If the object does not exist in the bucket.
        """
        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)
        self.buckets[bucket_id].remove(object_id)

    async def _init_multipart_upload(self, *, bucket_id: str, object_id: str) -> str:
        """Initiates a multipart upload procedure. Returns the upload ID.

        Raises:
            `MultiPartUploadAlreadyExistsError`: If an upload exists for the object.
            `BucketNotFoundError`: If the bucket does not exist.
        """
        await self._assert_no_multipart_upload(bucket_id=bucket_id, object_id=object_id)
        upload_id = str(uuid4())
        if object_id not in self.buckets[bucket_id]:
            self.uploads[bucket_id][object_id] = set()
        self.uploads[bucket_id][object_id].add(upload_id)
        return upload_id

    async def _get_part_upload_url(  # noqa: PLR0913
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        part_number: int,
        expires_after: int = 3600,
        part_md5: str | None = None,
    ) -> str:
        """
        Return a FAKE presigned part upload URL based on the bucket, object, and part no.

        This URL does not point to an actual resource or API, it is merely for testing.

        You may also specify a custom expiry duration in seconds (`expires_after`).
        Please note: the part number must be a non-zero, positive integer.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `MultiPartUploadNotFoundError`: If the upload ID doesn't exist.
            `MultipleActiveUploadsError`: If `assert_exclusiveness` is True and there
                are multiple open uploads for the object ID.
            `ValueError`: If `part_number` is less than 1 or exceeds MAX_FILE_PART_NUMBER.
        """
        if not 0 < part_number <= self.MAX_FILE_PART_NUMBER:
            raise ValueError(
                "The part number must be a non-zero positive integer"
                + f" smaller or equal to {self.MAX_FILE_PART_NUMBER}"
            )

        await self._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

        params = {
            "Bucket": bucket_id,
            "Key": object_id,
            "UploadId": upload_id,
            "PartNumber": part_number,
        }
        # add additional parameters if any were passed
        if part_md5:
            params["ContentMD5"] = part_md5
        return (
            f"https://s3.example.com/{bucket_id}/{object_id}/{upload_id}"
            + f"/part_no_{part_number}?{expires_after=}"
        )

    async def _abort_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> None:
        """Abort a multipart upload with the specified ID.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `MultiPartUploadNotFoundError`: If the upload ID doesn't exist.
            `MultipleActiveUploadsError`: If `assert_exclusiveness` is True and there are
                multiple open uploads for the object ID.
        """
        await self._assert_multipart_upload_exists(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            assert_exclusiveness=False,
        )
        self.uploads[bucket_id][object_id].remove(upload_id)

    async def _complete_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        anticipated_part_quantity: int | None = None,
        anticipated_part_size: int | None = None,
    ) -> None:
        """Completes a multipart upload with the specified ID."""
        # Calling this abort function because in the mock, the logic is identical to
        # what we would write here anyway -- in real life, completing an upload does not
        # involve the abort logic
        await self._abort_multipart_upload(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
        )
        self.buckets[bucket_id].add(object_id)

    async def _get_object_download_url(
        self, *, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns a FAKE presigned HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).

        This URL does not point to an actual resource or API, it is merely for testing.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectNotFoundError`: If the object does not exist in the bucket.
        """
        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)
        return f"https://s3.example.com/{bucket_id}/{object_id}?{expires_after=}"

    async def _get_object_etag(self, *, bucket_id: str, object_id: str) -> str:
        """Return the etag of an object, which is hardcoded to be the string
        `"etag_for_{object_id}"`.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectNotFoundError`: If the object does not exist in the bucket.
        """
        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)
        return f"etag_for_{object_id}"

    async def _get_object_size(self, *, bucket_id: str, object_id: str) -> int:
        """Returns the size of an object in bytes, which is hardcoded to `1024` for
        testing purposes.

        Raises:
            `BucketNotFoundError`: If the bucket does not exist.
            `ObjectNotFoundError`: If the object does not exist in the bucket.
        """
        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)
        return 1024

    async def _copy_object(
        self,
        *,
        source_bucket_id: str,
        source_object_id: str,
        dest_bucket_id: str,
        dest_object_id: str,
        abort_failed: bool = True,
    ) -> None:
        """Copy an object from one bucket (`source_bucket_id` and `source_object_id`) to
        another bucket (`dest_bucket_id` and `dest_object_id`).
        This operation always succeeds if the object exists in the source bucket and
        does not exist in the dest bucket.

        Raises:
            `BucketNotFoundError`: If the source or dest bucket does not exist.
            `ObjectAlreadyExistsError`: If the object already exists in the dest bucket.
            `ObjectNotFoundError`: If the object does not exist in the source bucket.
        """
        await self._assert_object_not_exists(
            bucket_id=dest_bucket_id, object_id=dest_object_id
        )
        await self._assert_object_exists(
            bucket_id=source_bucket_id, object_id=source_object_id
        )
        self.buckets[dest_bucket_id].add(dest_object_id)
