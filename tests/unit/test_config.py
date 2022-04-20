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

"""Test config parsing module"""
import os
import shutil
from pathlib import Path

import pytest

from hexkit.config import config_from_yaml
from tests.fixtures.config import BasicConfig, config_yamls, env_var_sets


def test_config_from_yaml():
    """Test that config yaml correctly overwrites
    default parameters"""

    config_yaml = config_yamls["basic"]

    # update config class with content of config yaml
    config_constructor = config_from_yaml()(BasicConfig)
    config = config_constructor(config_yaml=config_yaml.path)

    # compare to expected content:
    expected = BasicConfig(**config_yaml.content)
    assert config.dict() == expected


def test_config_from_env():
    """Test that env vars correctly overwrites
    default parameters"""
    env_var_fixture = env_var_sets["basic_complete"]
    with env_var_fixture:
        # update config class with content of config yaml and
        # from the env vars
        config_constructor = config_from_yaml()(BasicConfig)
        config = config_constructor()

    # compare to expected content:
    expected = BasicConfig(**env_var_fixture.env_vars)
    assert config.dict() == expected


def test_config_from_yaml_and_env():
    """Test that config yaml and env vars correctly overwrites
    default parameters"""

    config_yaml = config_yamls["basic"]
    env_var_fixture = env_var_sets["basic_partly"]

    with env_var_fixture:
        # update config class with content of config yaml and
        # from the env vars
        config_constructor = config_from_yaml()(BasicConfig)
        config = config_constructor(config_yaml=config_yaml.path)

    # compare to expected content:
    overwrite_params = {**config_yaml.content, **env_var_fixture.env_vars}
    expected = BasicConfig(**overwrite_params)
    assert config.dict() == expected


@pytest.mark.parametrize("cwd", [True, False])
def test_config_from_default_yaml(cwd: bool):
    """Test that default config yaml from home is correctly read"""

    base_dir = Path(os.getcwd()) if cwd else Path.home()
    prefix = "test_prefix"

    # copy basic config to default config location:
    config_yaml = config_yamls["basic"]
    default_yaml_path = base_dir / f".{prefix}.yaml"
    shutil.copy(config_yaml.path, default_yaml_path)

    # update config class with content of config yaml
    config_constructor = config_from_yaml(prefix=prefix)(BasicConfig)
    config = config_constructor()

    # cleanup default config yaml:
    os.remove(default_yaml_path)

    # compare to expected content:
    expected = BasicConfig(**config_yaml.content)
    assert config.dict() == expected


def test_config_from_default_yaml_via_env():
    """Test that default config yaml specified via an environment variable is correctly
    read"""

    prefix = "test_prefix"

    # set env var:
    config_yaml = config_yamls["basic"]
    os.environ[f"{prefix.upper()}_CONFIG_YAML"] = str(config_yaml.path)

    # update config class with content of config yaml
    config_constructor = config_from_yaml(prefix=prefix)(BasicConfig)
    config = config_constructor()

    # compare to expected content:
    expected = BasicConfig(**config_yaml.content)
    assert config.dict() == expected


def test_error_on_invalid_base_class():
    """Check that an exception is thrown if the class passed to the
    config decorator does not inherit from pydantic.BaseSettings
    """

    class WrongClass:
        """Not inheriting from pydantic.BaseSettings"""

    with pytest.raises(TypeError):
        config_from_yaml()(WrongClass)()
