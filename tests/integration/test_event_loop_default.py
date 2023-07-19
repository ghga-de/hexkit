# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Test module-scope joint fixture with default function-scope event loop"""

import pytest

from tests.fixtures.dummy_joint import (  # noqa: F401
    JointFixture,
    joint_fixture,
    s3_fixture,
)


@pytest.mark.xfail(strict=True)
@pytest.mark.asyncio
async def test_default_event_loop(joint_fixture: JointFixture):  # noqa: F811
    """Test that module-scope joint fixture with function-scope event loop breaks.

    We should get an error while trying to use a module-scope joint_fixture with the
    default event_loop fixture.
    """
    assert True
