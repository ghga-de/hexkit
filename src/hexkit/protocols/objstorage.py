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
#

"""Protocol for interacting with S3-like Object Storages."""

# ruff: noqa: PLR0913

import re
from abc import ABC, abstractmethod
from typing import NamedTuple, Optional

__all__ = ["ObjectStorageProtocol", "PresignedPostURL"]

DEFAULT_URL_EXPIRATION_PERIOD = 24 * 60 * 60  # default expiration time 24 hours


class PresignedPostURL(NamedTuple):
    """Container for presigned POST URLs along with additional metadata fields that
    should be attached as body data when sending the POST request.
    """

    url: str
    fields: dict[str, str]


class ObjectStorageProtocol(ABC):
    """Protocol for interacting with S3-like Object Storages."""

    # constants for multipart uploads:
    # (shall not be changed by provider implementations)
    DEFAULT_URL_EXPIRATION_PERIOD = DEFAULT_URL_EXPIRATION_PERIOD
    DEFAULT_PART_SIZE = 16 * 1024 * 1024
    MAX_FILE_PART_NUMBER = 10000

    # Public methods:
    # (Shall not be changed by provider implementations.)
    async def does_bucket_exist(self, bucket_id: str) -> bool:
        """Check whether a bucket with the specified ID (`bucket_id`) exists.
        Returns `True` if it exists and `False` otherwise.
        """
        self._validate_bucket_id(bucket_id)
        return await self._does_bucket_exist(bucket_id)

    async def create_bucket(self, bucket_id: str) -> None:
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """
        self._validate_bucket_id(bucket_id)
        await self._create_bucket(bucket_id)

    async def delete_bucket(
        self, bucket_id: str, *, delete_content: bool = False
    ) -> None:
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID. If `delete_content` is set to True, any contained objects
        will be deleted, if False (the default) a BucketNotEmptyError will be raised if
        the bucket is not empty.
        """
        self._validate_bucket_id(bucket_id)
        await self._delete_bucket(bucket_id, delete_content=delete_content)

    async def list_all_object_ids(self, bucket_id: str) -> list[str]:
        """Retrieve a list of IDs for all objects currently present in the specified bucket"""
        self._validate_bucket_id(bucket_id)
        return await self._list_all_object_ids(bucket_id=bucket_id)

    async def get_object_upload_url(
        self,
        *,
        bucket_id: str,
        object_id: str,
        expires_after: int = DEFAULT_URL_EXPIRATION_PERIOD,
        max_upload_size: Optional[int] = None,
    ) -> PresignedPostURL:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`) and
        a maximum size (bytes) for uploads (`max_upload_size`).
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._get_object_upload_url(
            bucket_id=bucket_id,
            object_id=object_id,
            expires_after=expires_after,
            max_upload_size=max_upload_size,
        )

    async def init_multipart_upload(
        self,
        *,
        bucket_id: str,
        object_id: str,
    ) -> str:
        """Initiates a multipart upload procedure. Returns the upload ID."""
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._init_multipart_upload(
            bucket_id=bucket_id, object_id=object_id
        )

    async def get_part_upload_url(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        part_number: int,
        expires_after: int = 3600,
        part_md5: Optional[str] = None,
    ) -> str:
        """Given a id of an instantiated multipart upload along with the corresponding
        bucket and object ID, it returns a presigned URL for uploading a file part with the
        specified number.
        Please note: the part number must be a non-zero, positive integer and parts
        should be uploaded in sequence.
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._get_part_upload_url(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            part_number=part_number,
            expires_after=expires_after,
            part_md5=part_md5,
        )

    async def abort_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> None:
        """Cancel a multipart upload with the specified ID. All uploaded content is
        deleted.
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        await self._abort_multipart_upload(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

    async def complete_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        anticipated_part_quantity: Optional[int] = None,
        anticipated_part_size: Optional[int] = None,
    ) -> None:
        """Completes a multipart upload with the specified ID. In addition to the
        corresponding bucket and object id, you also specify an anticipated part size
        and an anticipated part quantity.
        This ensures that exactly the specified number of parts exist and that all parts
        (except the last one) have the specified size.
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        await self._complete_multipart_upload(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            anticipated_part_quantity=anticipated_part_quantity,
            anticipated_part_size=anticipated_part_size,
        )

    async def get_object_download_url(
        self, *, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns a presigned HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._get_object_download_url(
            bucket_id=bucket_id, object_id=object_id, expires_after=expires_after
        )

    async def does_object_exist(
        self, *, bucket_id: str, object_id: str, object_md5sum: Optional[str] = None
    ) -> bool:
        """Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Returns `True` if checks succeed and `False` otherwise.
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._does_object_exist(
            bucket_id=bucket_id, object_id=object_id, object_md5sum=object_md5sum
        )

    async def get_object_etag(self, *, bucket_id: str, object_id: str) -> str:
        """Returns the etag of an object."""
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._get_object_etag(bucket_id=bucket_id, object_id=object_id)

    async def get_object_size(self, *, bucket_id: str, object_id: str) -> int:
        """Returns the size of an object in bytes."""
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        return await self._get_object_size(bucket_id=bucket_id, object_id=object_id)

    async def copy_object(
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

        If `abort_failed` is set to true (default), a failed copy operation tries to
        abort the ongoing multipart upload it created (if using multipart mode).
        This only works reliably as long as there are no other ongoing multipart operations for
        the same destination bucket and object ID, in which case this should be set to false.
        """
        self._validate_bucket_id(source_bucket_id)
        self._validate_object_id(source_object_id)
        self._validate_bucket_id(dest_bucket_id)
        self._validate_object_id(dest_object_id)
        await self._copy_object(
            source_bucket_id=source_bucket_id,
            source_object_id=source_object_id,
            dest_bucket_id=dest_bucket_id,
            dest_object_id=dest_object_id,
            abort_failed=abort_failed,
        )

    async def delete_object(self, *, bucket_id: str, object_id: str) -> None:
        """Delete an object with the specified id (`object_id`) in the bucket with the
        specified id (`bucket_id`).
        """
        self._validate_bucket_id(bucket_id)
        self._validate_object_id(object_id)
        await self._delete_object(bucket_id=bucket_id, object_id=object_id)

    # To be implemented by the provider:

    @abstractmethod
    async def _does_bucket_exist(self, bucket_id: str) -> bool:
        """
        Check whether a bucket with the specified ID (`bucket_id`) exists.
        Returns `True` if it exists and `False` otherwise.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _create_bucket(self, bucket_id: str) -> None:
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _delete_bucket(
        self, bucket_id: str, *, delete_content: bool = False
    ) -> None:
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID. If `delete_content` is set to True, any contained objects
        will be deleted, if False (the default) a BucketNotEmptyError will be raised if
        the bucket is not empty.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _list_all_object_ids(self, *, bucket_id: str) -> list[str]:
        """
        Retrieve a list of IDs for all objects currently present in the specified bucket

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _get_object_upload_url(
        self,
        *,
        bucket_id: str,
        object_id: str,
        expires_after: int = 86400,
        max_upload_size: Optional[int] = None,
    ) -> PresignedPostURL:
        """
        Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`) and
        a maximum size (bytes) for uploads (`max_upload_size`).

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _init_multipart_upload(
        self,
        *,
        bucket_id: str,
        object_id: str,
    ) -> str:
        """
        Initiates a multipart upload procedure. Returns the upload ID.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _get_part_upload_url(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        part_number: int,
        expires_after: int = 3600,
        part_md5: Optional[str] = None,
    ) -> str:
        """
        Given a id of an instantiated multipart upload along with the corresponding
        bucket and object ID, it returns a presigned URL for uploading a file part with the
        specified number.
        Please note: the part number must be a non-zero, positive integer and parts
        should be uploaded in sequence.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _abort_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> None:
        """
        Cancel a multipart upload with the specified ID. All uploaded content is
        deleted.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _complete_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        anticipated_part_quantity: Optional[int] = None,
        anticipated_part_size: Optional[int] = None,
    ) -> None:
        """
        Completes a multipart upload with the specified ID. In addition to the
        corresponding bucket and object id, you also specify an anticipated part size
        and an anticipated part quantity.
        This ensures that exactly the specified number of parts exist and that all parts
        (except the last one) have the specified size.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _get_object_download_url(
        self, *, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """
        Generates and returns a presigned HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _get_object_etag(self, *, bucket_id: str, object_id: str) -> str:
        """Return the etag of an object.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """

    @abstractmethod
    async def _get_object_size(self, *, bucket_id: str, object_id: str) -> int:
        """
        Returns the size of an object in bytes.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _does_object_exist(
        self, *, bucket_id: str, object_id: str, object_md5sum: Optional[str] = None
    ) -> bool:
        """
        Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Returns `True` if checks succeed and `False` otherwise.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _copy_object(
        self,
        *,
        source_bucket_id: str,
        source_object_id: str,
        dest_bucket_id: str,
        dest_object_id: str,
        abort_failed: bool = True,
    ) -> None:
        """
        Copy an object from one bucket (`source_bucket_id` and `source_object_id`) to
        another bucket (`dest_bucket_id` and `dest_object_id`).

        If `abort_failed` is set to true (default), a failed copy operation tries to
        abort the ongoing multipart upload it created (if using multipart mode).
        This only works reliably as long as there are no other ongoing multipart operations for
        the same destination bucket and object ID, in which case this should be set to false.

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    @abstractmethod
    async def _delete_object(self, *, bucket_id: str, object_id: str) -> None:
        """Delete an object with the specified id (`object_id`) in the bucket with the
        specified id (`bucket_id`).

        *To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

    # Validation logic for input parameter:
    # (is typically only used by the protocol but may also be used in
    # provider-specific code or overwritten by the provider)

    _re_bucket_id = re.compile(r"^[a-z0-9\-]{3,63}$")
    _re_bucket_id_msg = "must consist of 3-63 lowercase letters, digits or hyphens"

    @classmethod
    def _validate_bucket_id(cls, bucket_id: str):
        """Check whether a bucket id follows the recommended naming pattern.
        This is roughly based on:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
        Raises BucketIdValidationError if not valid.
        """
        if not cls._re_bucket_id.match(bucket_id):
            raise cls.BucketIdValidationError(
                bucket_id=bucket_id,
                reason=cls._re_bucket_id_msg,
            )
        if bucket_id.startswith("-") or bucket_id.endswith("-"):
            raise cls.BucketIdValidationError(
                bucket_id=bucket_id,
                reason="may not start or end with a hyphen",
            )

    _re_object_id = re.compile(r"^[a-zA-Z0-9\-_.]{3,255}$")
    _re_object_id_msg = (
        "must consist of 3-255 letters, digits, hyphens, underscores or dots"
    )

    @classmethod
    def _validate_object_id(cls, object_id: str):
        """Check whether an object id follows the recommended naming pattern.
        This is roughly based on (plus some additional restrictions):
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        Raises ObjectIdValidationError if not valid.

        Note that the default implementation does not allow object IDs to contain
        forward slashes (/) which are sometimes used to represent directories.
        Providers can override this method or the above constants to allow slashes,
        other characters or longer object names.
        """
        if not cls._re_object_id.match(object_id):
            raise cls.ObjectIdValidationError(
                object_id=object_id,
                reason=cls._re_object_id_msg,
            )
        if object_id.startswith(("-", ".")) or object_id.endswith(("-", ".")):
            raise cls.ObjectIdValidationError(
                object_id=object_id,
                reason="object names may not start or end with a hyphen or a dot",
            )

    # Exceptions that may be used by implementation:

    class ObjectStorageProtocolError(RuntimeError):
        """Generic base exception for all custom errors used by this protocol."""

    class BucketError(ObjectStorageProtocolError):
        """Generic base exception for error that occur while handling buckets."""

    class BucketNotFoundError(BucketError):
        """Thrown when trying to access a bucket with an ID that doesn't exist."""

        def __init__(self, bucket_id: Optional[str]):
            with_id = f" with ID '{bucket_id}'" if bucket_id else ""
            message = f"The bucket{with_id} does not exist."
            super().__init__(message)

    class BucketAlreadyExistsError(BucketError):
        """Thrown when trying to create a bucket with an ID that already exists."""

        def __init__(self, bucket_id: Optional[str]):
            with_id = f" with ID '{bucket_id}'" if bucket_id else ""
            message = f"The bucket{with_id} already exists."
            super().__init__(message)

    class BucketNotEmptyError(BucketError):
        """Thrown when trying to delete a bucket that is not empty."""

        def __init__(self, bucket_id: Optional[str]):
            with_id = f" with ID '{bucket_id}'" if bucket_id else ""
            super().__init__(f"The bucket{with_id} is not empty.")

    class ObjectError(ObjectStorageProtocolError):
        """Generic base exception for error that occur while handling file objects."""

    class ObjectNotFoundError(ObjectError):
        """Thrown when trying to access a bucket with an ID that doesn't exist."""

        def __init__(
            self, bucket_id: Optional[str] = None, object_id: Optional[str] = None
        ):
            with_id = f" with ID '{object_id}'" if object_id else ""
            in_bucket = f" in bucket with ID '{bucket_id}'" if bucket_id else ""
            message = f"The object{with_id}{in_bucket} does not exist."
            super().__init__(message)

    class ObjectAlreadyExistsError(ObjectError):
        """Thrown when trying to access a file with an ID that doesn't exist."""

        def __init__(
            self, bucket_id: Optional[str] = None, object_id: Optional[str] = None
        ):
            with_id = f" with ID '{object_id}'" if object_id else ""
            in_bucket = f" in bucket with ID '{bucket_id}'" if bucket_id else ""
            message = f"The object{with_id}{in_bucket} already exists."
            super().__init__(message)

    class BucketIdValidationError(BucketError):
        """Thrown when a bucket ID is not valid."""

        def __init__(self, bucket_id: str, reason: Optional[str]):
            with_reason = f": {reason}." if reason else "."
            message = f"The specified bucket ID '{bucket_id}' is not valid{with_reason}"
            super().__init__(message)

    class ObjectIdValidationError(ObjectError):
        """Thrown when an object ID is not valid."""

        def __init__(self, object_id: str, reason: Optional[str]):
            with_reason = f": {reason}." if reason else "."
            message = f"The specified object ID '{object_id}' is not valid{with_reason}"
            super().__init__(message)

    class MultiPartUploadError(ObjectError):
        """Thrown when a confirmation of an upload is rejected."""

    class MultiPartUploadAlreadyExistsError(MultiPartUploadError):
        """Thrown when trying to create a multipart upload for an object for which another
        upload is already active.
        """

        def __init__(self, bucket_id: str, object_id: str):
            message = (
                f"Failed to initiate a multi-part upload for object '{object_id}' in bucket"
                + f" '{bucket_id}. Another upload is already ongoing for that file."
            )
            super().__init__(message)

    class MultipleActiveUploadsError(MultiPartUploadError):
        """Thrown when multiple active multi-part uploads are detected for the same object."""

        def __init__(self, bucket_id: str, object_id: str, upload_ids: list[str]):
            message = (
                f"Multiple active multi-part uploads were detected for object '{object_id}'"
                + f" in bucket '{bucket_id}. Another upload is already ongoing for that"
                + f" file. The IDs of the active uploads are: {','.join(upload_ids)}"
            )
            super().__init__(message)

    class MultiPartUploadNotFoundError(MultiPartUploadError):
        """Thrown when a upload with the specified upload, bucket, and object id was not found."""

        def __init__(
            self,
            upload_id: str,
            bucket_id: str,
            object_id: str,
            details: Optional[str] = None,
        ):
            with_details = f": {details}." if details else "."
            message = (
                f"The multi-part upload with ID '{upload_id}' for object '{object_id}'"
                + f" in bucket '{bucket_id}' could not be found{with_details}"
            )
            super().__init__(message)

    class MultiPartUploadConfirmError(MultiPartUploadError):
        """Thrown when a confirmation of an upload is rejected."""

        def __init__(
            self,
            upload_id: str,
            bucket_id: str,
            object_id: str,
            reason: Optional[str] = None,
        ):
            with_reason = f": {reason}." if reason else "."
            message = (
                f"The confirmation of multi-part upload '{upload_id}' for object"
                + f" '{object_id}' in bucket '{bucket_id} was rejected{with_reason}"
            )
            super().__init__(message)

    class MultiPartUploadAbortError(MultiPartUploadError):
        """Thrown when failed to abort a multi-part upload."""

        def __init__(
            self,
            upload_id: str,
            bucket_id: str,
            object_id: str,
        ):
            message = (
                f"Failed to abort the multi-part upload '{upload_id}' for object"
                + f" '{object_id}' in bucket '{bucket_id}'. An ongoing part upload might"
                + " be a reason. Please complete all part upload and try to abort again."
            )
            super().__init__(message)
