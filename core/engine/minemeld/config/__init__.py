#  Copyright 2015 Palo Alto Networks, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import sys
import time
import os
import os.path
import logging
import shutil
import re
from typing import (
    TypedDict, List, Any,
    Dict, Set, Optional,
    Type, Tuple, TextIO,
    NoReturn, Union
)

import yaml

import minemeld.loader

from .model import TMineMeldNodeConfig, MineMeldConfig, TMineMeldConfig, detect_cycles
from .gc import destroy_old_nodes
from .prototypes import resolve
from .load import load_config_from_dir, load_and_validate_config_from_file


__all__ = ['load', 'resolve', 'MineMeldConfig']


# disables construction of timestamp objects
yaml.SafeLoader.add_constructor(
    'tag:yaml.org,2002:timestamp',
    yaml.SafeLoader.construct_yaml_str
)


LOG = logging.getLogger(__name__)


def load_config(config_path: str) -> 'MineMeldConfig':
    if os.path.isdir(config_path):
        return load_config_from_dir(config_path)

    # this is just a file, as we can't do a delta
    # we just load it and mark all the nodes as added
    valid, config = load_and_validate_config_from_file(config_path)
    if not valid:
        raise RuntimeError('Invalid config')
    assert config is not None
    config.compute_changes(None)

    return config
