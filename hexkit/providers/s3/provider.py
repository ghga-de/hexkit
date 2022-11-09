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

"""S3-based provider implementing the ObjectStorageProtocol.

Utilities for testing are located in `./testutils.py`.
"""

import asyncio
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import boto3
import botocore.client
import botocore.config
import botocore.configloader
import botocore.exceptions
from boto3.s3.transfer import TransferConfig
from pydantic import BaseSettings, Field, SecretStr

from hexkit.protocols.objstorage import ObjectStorageProtocol, PresignedPostURL
from hexkit.utils import calc_part_size

__all__ = ["ObjectStorageProtocol", "PresignedPostURL"]


class S3Config(BaseSettings):
    """S3-specific config params.
    Inherit your config class from this class if you need
    to talk to an S3 service in the backend.

    Args:
        s3_endpoint_url (str): The URL to the S3 endpoint.
        s3_access_key_id (str):
            Part of credentials for login into the S3 service. See:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        s3_secret_access_key (str):
            Part of credentials for login into the S3 service. See:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        s3_session_token (Optional[str]):
            Optional part of credentials for login into the S3 service. See:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        aws_config_ini (Optional[Path]):
            Path to a config file for specifying more advanced S3 parameters.
            This should follow the format described here:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file
            Defaults to None.
    """

    s3_endpoint_url: str = Field(
        ..., example="http://localhost:4566", description="URL to the S3 API."
    )
    s3_access_key_id: str = Field(
        ...,
        example="my-access-key-id",
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    s3_secret_access_key: SecretStr = Field(
        ...,
        example="my-secret-access-key",
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    s3_session_token: Optional[SecretStr] = Field(
        None,
        example="my-session-token",
        description=(
            "Part of credentials for login into the S3 service. See:"
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        ),
    )
    aws_config_ini: Optional[Path] = Field(
        None,
        example="~/.aws/config",
        description=(
            "Path to a config file for specifying more advanced S3 parameters."
            + " This should follow the format described here:"
            # pylint: disable=line-too-long
            + " https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file"
        ),
    )


@lru_cache
def read_aws_config_ini(aws_config_ini: Path) -> botocore.config.Config:
    """
    Reads an INI-formatted AWS config file (see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file)
    and returns an botocore.config.Config object.
    """

    config_profile = botocore.configloader.load_config(config_filename=aws_config_ini)
    return botocore.config.Config(**config_profile)


class S3ObjectStorage(
    ObjectStorageProtocol
):  # pylint: disable=too-many-instance-attributes
    """
    S3-based provider implementing the ObjectStorageProtocol.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        config: S3Config,
    ):
        """Initialize with parameters needed to connect to the S3 storage

        The arguments are adapted from the boto3 library.
        Please have a look here for more descriptions:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client

        Args:
            config (S3Config): Config parameters specified using the S3Config model.
        """

        self._config = config

        self.endpoint_url = config.s3_endpoint_url

        self._advanced_config = (
            None
            if config.aws_config_ini is None
            else read_aws_config_ini(config.aws_config_ini)
        )

        session_token = (
            None
            if self._config.s3_session_token is None
            else self._config.s3_session_token.get_secret_value()
        )
        self._client = boto3.client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key.get_secret_value(),
            aws_session_token=session_token,
            config=self._advanced_config,
        )

        self._resource = boto3.resource(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key.get_secret_value(),
            aws_session_token=session_token,
            config=self._advanced_config,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    @staticmethod
    def _format_s3_error_code(error_code: str):
        """Format a message to describe an S3 error code."""

        return f"S3 error with code: '{error_code}'"

    @classmethod
    def _translate_s3_client_errors(  # noqa: C901
        cls,
        source_exception: botocore.exceptions.ClientError,
        *,
        upload_id: Optional[str] = None,
        bucket_id: Optional[str] = None,
        object_id: Optional[str] = None,
    ) -> Exception:
        """
        Translates S3 client errors based on their error codes into exceptions from the
        ObjectStorageProtocol modules
        """

        error_code = source_exception.response["Error"]["Code"]
        exception: Exception

        # try to exactly match the error code:
        if error_code == "NoSuchBucket":
            exception = cls.BucketNotFoundError(bucket_id=bucket_id)
        elif error_code == "BucketAlreadyExists":
            exception = cls.BucketAlreadyExistsError(bucket_id=bucket_id)
        elif error_code == "NoSuchKey":
            exception = cls.ObjectNotFoundError(
                bucket_id=bucket_id, object_id=object_id
            )
        elif error_code == "BucketNotEmpty":
            exception = cls.BucketNotEmptyError(bucket_id=bucket_id)
        elif error_code == "ObjectAlreadyInActiveTierError":
            exception = cls.ObjectAlreadyExistsError(
                bucket_id=bucket_id, object_id=object_id
            )
        elif error_code == "ObjectAlreadyInActiveTierError":
            exception = cls.ObjectAlreadyExistsError(
                bucket_id=bucket_id, object_id=object_id
            )
        elif error_code == "NoSuchUpload":
            if upload_id is None or bucket_id is None or object_id is None:
                raise ValueError()
            exception = cls.MultiPartUploadNotFoundError(
                upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
            )
        else:
            # exact match not found, match by keyword:
            if "Bucket" in error_code:
                exception = cls.BucketError(cls._format_s3_error_code(error_code))
            elif "Object" in error_code or "Key" in error_code:
                exception = cls.ObjectError(cls._format_s3_error_code(error_code))
            else:
                # if nothing matches, return a generic error:
                exception = cls.ObjectStorageProtocolError(
                    cls._format_s3_error_code(error_code)
                )

        return exception

    async def _does_bucket_exist(self, bucket_id: str) -> bool:
        """Check whether a bucket with the specified ID (`bucket_id`) exists.
        Return `True` if it exists and `False` otherwise.
        """

        try:
            bucket_list = await asyncio.to_thread(self._client.list_buckets)
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id
            ) from error

        return any(bucket["Name"] == bucket_id for bucket in bucket_list["Buckets"])

    async def _assert_bucket_exists(self, bucket_id: str) -> None:
        """Checks if the bucket with specified ID (`bucket_id`) exists and throws an
        BucketNotFoundError otherwise.
        """

        if not await self.does_bucket_exist(bucket_id):
            raise self.BucketNotFoundError(bucket_id=bucket_id)

    async def _assert_bucket_not_exists(self, bucket_id: str) -> None:
        """Checks if the bucket with specified ID (`bucket_id`) exists. If so, it throws
        an BucketAlreadyExistsError.
        """

        if await self.does_bucket_exist(bucket_id):
            raise self.BucketAlreadyExistsError(bucket_id=bucket_id)

    async def _create_bucket(self, bucket_id: str) -> None:
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """

        await self._assert_bucket_not_exists(bucket_id)

        try:
            await asyncio.to_thread(self._client.create_bucket, Bucket=bucket_id)
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id
            ) from error

    async def _delete_bucket(
        self, bucket_id: str, *, delete_content: bool = False
    ) -> None:
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID. If `delete_content` is set to True, any contained objects
        will be deleted, if False (the default) a BucketNotEmptyError will be raised if
        the bucket is not empty.
        """

        await self._assert_bucket_exists(bucket_id)

        try:
            bucket = self._resource.Bucket(bucket_id)
            content = await asyncio.to_thread(bucket.objects.all)
            if delete_content:
                await asyncio.to_thread(content.delete)
            await asyncio.to_thread(bucket.delete)
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id
            ) from error

    async def _does_object_exist(
        self, *, bucket_id: str, object_id: str, object_md5sum: Optional[str] = None
    ) -> bool:
        """Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Return `True` if checks succeed and `False` otherwise.
        """

        if object_md5sum is not None:
            raise NotImplementedError("Md5 checking is not yet implemented.")

        try:
            _ = await asyncio.to_thread(
                self._client.head_object,
                Bucket=bucket_id,
                Key=object_id,
            )
        except botocore.exceptions.ClientError:
            return False

        return True

    async def _assert_object_exists(self, *, bucket_id: str, object_id: str) -> None:
        """Checks if the file with specified ID (`object_id`) exists in the bucket with
        the specified ID (`bucket_id`) and throws an ObjectNotFoundError otherwise.
        If the bucket does not exist it throws a BucketNotFoundError.
        """

        # first check if bucket exists:
        await self._assert_bucket_exists(bucket_id)

        if not await self.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            raise self.ObjectNotFoundError(bucket_id=bucket_id, object_id=object_id)

    async def _assert_object_not_exists(
        self, *, bucket_id: str, object_id: str
    ) -> None:
        """Checks if the file with specified ID (`object_id`) exists in the bucket with
        the specified ID (`bucket_id`). If so, it throws an ObjectAlreadyExistsError.
        If the bucket does not exist it throws a BucketNotFoundError.
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
        max_upload_size: Optional[int] = None,
    ) -> PresignedPostURL:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`) and
        a maximum size (bytes) for uploads (`max_upload_size`).
        """

        await self._assert_object_not_exists(bucket_id=bucket_id, object_id=object_id)

        conditions = (
            []
            if max_upload_size is None
            else [
                # set upload size limit:
                ["content-length-range", 0, max_upload_size],
            ]
        )

        try:
            presigned_url = await asyncio.to_thread(
                self._client.generate_presigned_post,
                Bucket=bucket_id,
                Key=object_id,
                Conditions=conditions,
                ExpiresIn=expires_after,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        return PresignedPostURL(
            url=presigned_url["url"], fields=presigned_url["fields"]
        )

    async def _list_multipart_upload_for_object(
        self, *, bucket_id: str, object_id: str
    ) -> list[str]:
        """Lists all active multipart uploads for the given object. Returns a list of
        their IDs.

        (S3 allows multiple ongoing multi-part uploads.)
        """

        try:
            uploads_info = await asyncio.to_thread(
                self._client.list_multipart_uploads,
                Bucket=bucket_id,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        upload_list = uploads_info.get("Uploads", [])
        return [
            upload["UploadId"] for upload in upload_list if upload["Key"] == object_id
        ]

    async def _assert_no_multipart_upload(self, *, bucket_id: str, object_id: str):
        """Ensure that there are no active multi-part uploads for the given object."""

        upload_ids = await self._list_multipart_upload_for_object(
            bucket_id=bucket_id, object_id=object_id
        )
        if len(upload_ids) > 0:
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
        to the specified object and bucket. Otherwise, raises MultiPartUploadNotFoundError.

        By default, also verifies that this upload is the only upload active for
        that file. Otherwise, raises MultipleActiveUploadsError.
        """

        upload_ids = await self._list_multipart_upload_for_object(
            bucket_id=bucket_id, object_id=object_id
        )
        n_uploads = len(upload_ids)

        if assert_exclusiveness and n_uploads > 1:
            raise self.MultipleActiveUploadsError(
                bucket_id=bucket_id, object_id=object_id, upload_ids=upload_ids
            )

        if upload_id not in upload_ids:
            raise self.MultiPartUploadNotFoundError(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            )

    async def _init_multipart_upload(self, *, bucket_id: str, object_id: str) -> str:
        """Initiates a mulipart upload procedure. Returns the upload ID."""

        await self._assert_no_multipart_upload(bucket_id=bucket_id, object_id=object_id)

        try:
            response = await asyncio.to_thread(
                self._client.create_multipart_upload, Bucket=bucket_id, Key=object_id
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        return response["UploadId"]

    async def _get_part_upload_url(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        part_number: int,
        expires_after: int = 3600,
    ) -> str:
        """Given a id of an instantiated multipart upload along with the corresponding
        bucket and object ID, it returns a presigned URL for uploading a file part with the
        specified number.
        You may also specify a custom expiry duration in seconds (`expires_after`).
        Please note: the part number must be a non-zero, positive integer and parts
        should be uploaded in sequence.
        """

        if not 0 < part_number <= self.MAX_FILE_PART_NUMBER:
            raise ValueError(
                "The part number must be a non-zero positive integer"
                + f" smaller or equal to {self.MAX_FILE_PART_NUMBER}"
            )

        await self._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

        try:
            return await asyncio.to_thread(
                self._client.generate_presigned_url,
                ClientMethod="upload_part",
                Params={
                    "Bucket": bucket_id,
                    "Key": object_id,
                    "UploadId": upload_id,
                    "PartNumber": part_number,
                },
                ExpiresIn=expires_after,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error,
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            ) from error

    async def _get_parts_info(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> dict:
        """Get information on parts uploaded as part of the specified multi-part upload."""

        await self._assert_multipart_upload_exists(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        )

        try:
            response_iter = await asyncio.to_thread(
                self._client.get_paginator("list_parts").paginate,
                Bucket=bucket_id,
                Key=object_id,
                UploadId=upload_id,
            )
            complete_response: dict[str, Any] = {}
            for response_page in response_iter:
                if not complete_response:
                    complete_response = response_page
                    continue
                complete_response["Parts"].extend(response_page["Parts"])
            return complete_response
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error,
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            ) from error

    # pylint: disable=too-many-arguments
    async def _check_uploaded_parts(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        parts_info: dict,
        anticipated_part_quantity: Optional[int] = None,
        anticipated_part_size: Optional[int] = None,
    ) -> None:
        """Check size and quantity of parts"""

        # check the part quantity:
        parts = parts_info.get("Parts")
        if parts is None or len(parts) == 0:
            raise self.MultiPartUploadConfirmError(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                reason="Zero parts received.",
            )

        part_quantity = len(parts)
        if (
            anticipated_part_quantity is not None
            and part_quantity != anticipated_part_quantity
        ):
            raise self.MultiPartUploadConfirmError(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                reason=f"Found {part_quantity} parts but expected"
                + f" {anticipated_part_quantity}.",
            )

        # check anticipated part size:
        first_part_size = parts[0]["Size"]
        last_part_size = parts[-1]["Size"]
        if anticipated_part_size is not None:
            # if we have only one part, this is not required
            if len(parts) > 1 and first_part_size != anticipated_part_size:
                raise self.MultiPartUploadConfirmError(
                    upload_id=upload_id,
                    bucket_id=bucket_id,
                    object_id=object_id,
                    reason=f"The first part has a size of {first_part_size} bytes but"
                    + f" expected {anticipated_part_quantity} bytes.",
                )
            if last_part_size > anticipated_part_size:
                raise self.MultiPartUploadConfirmError(
                    upload_id=upload_id,
                    bucket_id=bucket_id,
                    object_id=object_id,
                    reason=f"The last part has a size of {last_part_size} bytes which"
                    + " is larger than the anticipated size of"
                    + f" {anticipated_part_quantity} bytes.",
                )

        # check if the last part is not larger than the first one:
        if last_part_size > first_part_size:
            raise self.MultiPartUploadConfirmError(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                reason=f"The last part has a size of {last_part_size} bytes which"
                + " is larger than the size of the first part which was"
                + f" {first_part_size} bytes.",
            )

        # check if all parts (except the last one) conform to the size of the first one:
        for part in parts[1 : part_quantity - 1]:
            if part["Size"] != first_part_size:
                raise self.MultiPartUploadConfirmError(
                    upload_id=upload_id,
                    bucket_id=bucket_id,
                    object_id=object_id,
                    reason=f"Part number {part['PartNumber']} has a size of"
                    + f" {part['Size']} bytes which is different than the size of the"
                    + f" first part which had only {first_part_size}.",
                )

    # pylint: disable=too-many-arguments
    async def _abort_multipart_upload(
        self,
        *,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> None:
        """Abort a multipart upload with the specified ID. All uploaded content is
        deleted.
        """

        await self._assert_multipart_upload_exists(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            assert_exclusiveness=False,
        )
        # Exclusiveness is not enforced here since the abortion of an upload might be
        # used to resolve the invalid state of multiple uploads for the same object.

        try:
            await asyncio.to_thread(
                self._client.abort_multipart_upload,
                Bucket=bucket_id,
                Key=object_id,
                UploadId=upload_id,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error,
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            ) from error

        # verify that the abortion was successful as recommended by the boto3
        # documentation:
        # (Please see here for more details:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=abort_multipart_upload#S3.Client.abort_multipart_upload
        # )
        try:
            parts_info = await self._get_parts_info(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            )
        except self.MultiPartUploadNotFoundError:
            # this is proof enough that the upload was aborted:
            return

        # verify that no parts are remaining:
        parts = parts_info.get("Parts")
        if parts is not None and len(parts) > 0:
            raise self.MultiPartUploadAbortError(
                upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
            )

    # pylint: disable=too-many-arguments
    async def _complete_multipart_upload(
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

        parts_info = await self._get_parts_info(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
        )

        await self._check_uploaded_parts(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            parts_info=parts_info,
            anticipated_part_quantity=anticipated_part_quantity,
            anticipated_part_size=anticipated_part_size,
        )

        # construct eTags list:
        parts = parts_info.get("Parts", [])
        part_etags = [
            {"ETag": part["ETag"], "PartNumber": part["PartNumber"]} for part in parts
        ]

        # confirm the upload:
        try:
            await asyncio.to_thread(
                self._client.complete_multipart_upload,
                Bucket=bucket_id,
                Key=object_id,
                MultipartUpload={"Parts": part_etags},
                UploadId=upload_id,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error,
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
            ) from error

    async def _get_object_download_url(
        self, *, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns a presigned HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """

        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)

        try:
            presigned_url = await asyncio.to_thread(
                self._client.generate_presigned_url,
                "get_object",
                Params={"Bucket": bucket_id, "Key": object_id},
                ExpiresIn=expires_after,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        return presigned_url

    async def _get_object_metadata(
        self, *, bucket_id: str, object_id: str
    ) -> dict[str, Any]:
        """
        Returns object metadata without downloading the actual object.
        """
        try:
            metadata = await asyncio.to_thread(
                self._client.head_object,
                Bucket=bucket_id,
                Key=object_id,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(error) from error

        return metadata

    async def _copy_object(
        self,
        *,
        source_bucket_id: str,
        source_object_id: str,
        dest_bucket_id: str,
        dest_object_id: str,
    ) -> None:
        """Copy an object from one bucket (`source_bucket_id` and `source_object_id`) to
        another bucket (`dest_bucket_id` and `dest_object_id`).
        """

        await self._assert_object_exists(
            bucket_id=source_bucket_id, object_id=source_object_id
        )
        await self._assert_object_not_exists(
            bucket_id=dest_bucket_id, object_id=dest_object_id
        )

        source_metadata = await self._get_object_metadata(
            bucket_id=source_bucket_id, object_id=source_object_id
        )
        file_size = source_metadata["ContentLength"]
        part_size = calc_part_size(file_size=file_size)

        transfer_config = TransferConfig(
            multipart_threshold=8 * 1024**2, multipart_chunksize=part_size
        )
        try:
            copy_source = {
                "Bucket": source_bucket_id,
                "Key": source_object_id,
            }
            await asyncio.to_thread(
                self._client.copy,
                CopySource=copy_source,
                Bucket=dest_bucket_id,
                Key=dest_object_id,
                Config=transfer_config,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(error) from error

    async def _delete_object(self, *, bucket_id: str, object_id: str) -> None:
        """Delete an object with the specified id (`object_id`) in the bucket with the
        specified id (`bucket_id`).
        """

        await self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)

        try:
            await asyncio.to_thread(
                self._client.delete_object,
                Bucket=bucket_id,
                Key=object_id,
            )
        except botocore.exceptions.ClientError as error:
            raise self._translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error
