import os.path
import logging
from typing import (
    List
)

import yaml

import minemeld.loader
from .minemeldconfig import TMineMeldNodeConfig, MineMeldConfig

LOG = logging.getLogger(__name__)

PROTOTYPE_ENV = 'MINEMELD_PROTOTYPE_PATH'

# disables construction of timestamp objects
yaml.SafeLoader.add_constructor(
    'tag:yaml.org,2002:timestamp',
    yaml.SafeLoader.construct_yaml_str
)

def _load_node_prototype(protoname: str, paths: List[str]) -> TMineMeldNodeConfig:
    proto_module, proto_name = protoname.rsplit('.', 1)

    pmodule = None
    pmprotos = {}
    for p in paths:
        pmpath = os.path.join(p, proto_module+'.yml')

        try:
            with open(pmpath, 'r') as pf:
                pmodule = yaml.safe_load(pf)

                if pmodule is None:
                    pmodule = {}
        except IOError:
            pmodule = None
            continue

        pmprotos = pmodule.get('prototypes', {})

        if proto_name not in pmprotos:
            pmodule = None
            continue

        if 'class' not in pmprotos[proto_name]:
            pmodule = None
            continue

        return pmprotos[proto_name]

    raise RuntimeError('Unable to load prototype %s: '
                       ' not found' % (protoname))


def resolve(config: 'MineMeldConfig') -> bool:
    # retrieve prototype dir from environment
    # used for main library and local library
    paths_list = os.getenv(PROTOTYPE_ENV, None)
    if paths_list is None:
        raise RuntimeError('Unable to load prototypes: %s '
                           'environment variable not set' %
                           (PROTOTYPE_ENV))
    paths = paths_list.split(':')

    # add prototype dirs from extension to paths
    prototypes_entrypoints = minemeld.loader.map(
        minemeld.loader.MM_PROTOTYPES_ENTRYPOINT)
    for epname, mmep in prototypes_entrypoints.items():
        if not mmep.loadable:
            LOG.info('Prototypes entrypoint {} not loadable'.format(epname))
            continue

        try:
            ep = mmep.ep.load()
            # we add prototype paths in front, to let extensions override default protos
            paths.insert(0, ep())

        except:
            LOG.exception(
                'Exception retrieving path from prototype entrypoint {}'.format(
                    epname)
            )

    # resolve all prototypes
    valid = True

    nodes_config = config.nodes
    for _, nconfig in nodes_config.items():
        if 'prototype' in nconfig:
            try:
                nproto = _load_node_prototype(nconfig['prototype'], paths)
            except RuntimeError as e:
                LOG.error('Error loading prototype {}: {}'.format(
                    nconfig['prototype'],
                    str(e)
                ))
                valid = False
                continue

            nconfig.pop('prototype')

            nconfig['class'] = nproto['class']
            nproto_config = nproto.get('config', {})
            nproto_config.update(
                nconfig.get('config', {})
            )
            nconfig['config'] = nproto_config

    return valid
