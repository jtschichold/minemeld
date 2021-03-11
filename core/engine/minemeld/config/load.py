import os.path
import logging
import sys
import shutil
import time
from typing import (
    Tuple, TextIO, Optional
)

import yaml

from .minemeldconfig import TMineMeldConfig, MineMeldConfig
from .prototypes import resolve
from .gc import destroy_old_nodes


# disables construction of timestamp objects
yaml.SafeLoader.add_constructor(
    'tag:yaml.org,2002:timestamp',
    yaml.SafeLoader.construct_yaml_str
)


LOG = logging.getLogger(__name__)
COMMITTED_CONFIG = 'committed-config.yml'
RUNNING_CONFIG = 'running-config.yml'


def _load_config_from_file(path: str, f: TextIO) -> Tuple[bool, 'MineMeldConfig']:
    valid = True
    config: Optional[TMineMeldConfig] = yaml.safe_load(f)

    if not isinstance(config, dict) and config is not None:
        raise ValueError('Invalid config YAML type')

    return valid, MineMeldConfig.from_dict(path, config)


def load_and_validate_config_from_file(path: str) -> Tuple[bool, Optional['MineMeldConfig']]:
    valid = False
    config = None

    if os.path.isfile(path):
        try:
            with open(path, 'r') as cf:
                valid, config = _load_config_from_file(path, cf)
            if not valid:
                LOG.error('Invalid config file {}'.format(path))
        except (RuntimeError, IOError):
            LOG.exception(
                'Error loading config {}, config ignored'.format(path)
            )
            valid, config = False, None

    if valid and config is not None:
        valid = resolve(config)

    if valid and config is not None:
        vresults = config.validate()
        if len(vresults) != 0:
            LOG.error('Invalid config {}: {}'.format(
                path,
                ', '.join(vresults)
            ))
            valid = False

    return valid, config


def load_config_from_dir(path: str) -> 'MineMeldConfig':
    ccpath = os.path.join(
        path,
        COMMITTED_CONFIG
    )
    rcpath = os.path.join(
        path,
        RUNNING_CONFIG
    )

    ccvalid, cconfig = load_and_validate_config_from_file(ccpath)
    rcvalid, rcconfig = load_and_validate_config_from_file(rcpath)

    if not rcvalid and not ccvalid:
        # both running and canidate are not valid
        print(
            "At least one of", RUNNING_CONFIG,
            "or", COMMITTED_CONFIG,
            "should exist in", path,
            file=sys.stderr
        )
        sys.exit(1)

    elif rcvalid and not ccvalid:
        assert rcconfig is not None
        # running is valid but candidate is not
        return rcconfig

    elif not rcvalid and ccvalid:
        assert cconfig is not None
        # candidate is valid while running is not
        LOG.info('Switching to candidate config')
        cconfig.compute_changes(rcconfig)
        LOG.info('Changes in config: {!r}'.format(cconfig.changes))
        destroy_old_nodes(cconfig)
        if rcconfig is not None:
            shutil.copyfile(
                rcpath,
                '{}.{}'.format(rcpath, int(time.time()))
            )
        shutil.copyfile(ccpath, rcpath)
        return cconfig

    elif rcvalid and ccvalid:
        assert cconfig is not None
        assert rcconfig is not None
        LOG.info('Switching to candidate config')
        cconfig.compute_changes(rcconfig)
        LOG.info('Changes in config: {!r}'.format(cconfig.changes))
        destroy_old_nodes(cconfig)
        shutil.copyfile(
            rcpath,
            '{}.{}'.format(rcpath, int(time.time()))
        )
        shutil.copyfile(ccpath, rcpath)
        return cconfig

    raise RuntimeError("We should not be here!!")


def load(config_path: str) -> 'MineMeldConfig':
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