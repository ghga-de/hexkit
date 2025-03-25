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

"""S3-related test fixtures and support functions."""

from ._fixtures import (
    LOCALSTACK_IMAGE,
    MEBIBYTE,
    TEST_FILE_DIR,
    TEST_FILE_PATHS,
    FederatedS3Fixture,
    FileObject,
    S3ContainerFixture,
    S3Fixture,
    S3MultiContainerFixture,
    calc_md5,
    clean_federated_s3_fixture,
    clean_s3_fixture,
    federated_s3_fixture,
    get_clean_federated_s3_fixture,
    get_clean_s3_fixture,
    get_persistent_federated_s3_fixture,
    get_persistent_s3_fixture,
    get_s3_container_fixture,
    get_s3_multi_container_fixture,
    persistent_federated_s3_fixture,
    persistent_s3_fixture,
    populate_storage,
    s3_container_fixture,
    s3_fixture,
    s3_multi_container_fixture,
    temp_file_object,
    tmp_file,
    upload_file,
)
from ._typical_workflow import typical_workflow
from ._utils import (
    check_part_size,
    download_and_check_test_file,
    multipart_upload_file,
    upload_part,
    upload_part_of_size,
    upload_part_via_url,
)

__all__ = [
    "LOCALSTACK_IMAGE",
    "MEBIBYTE",
    "TEST_FILE_DIR",
    "TEST_FILE_PATHS",
    "FederatedS3Fixture",
    "FileObject",
    "S3ContainerFixture",
    "S3Fixture",
    "S3MultiContainerFixture",
    "calc_md5",
    "check_part_size",
    "clean_federated_s3_fixture",
    "clean_s3_fixture",
    "download_and_check_test_file",
    "federated_s3_fixture",
    "get_clean_federated_s3_fixture",
    "get_clean_s3_fixture",
    "get_persistent_federated_s3_fixture",
    "get_persistent_s3_fixture",
    "get_s3_container_fixture",
    "get_s3_multi_container_fixture",
    "multipart_upload_file",
    "persistent_federated_s3_fixture",
    "persistent_s3_fixture",
    "populate_storage",
    "s3_container_fixture",
    "s3_fixture",
    "s3_multi_container_fixture",
    "temp_file_object",
    "tmp_file",
    "typical_workflow",
    "upload_file",
    "upload_part",
    "upload_part_of_size",
    "upload_part_via_url",
]
