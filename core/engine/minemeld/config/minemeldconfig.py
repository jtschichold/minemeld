from collections import namedtuple
from typing import (
    TypedDict, List, Any,
    Dict, Set, Optional,
    Type, Tuple, TextIO,
    NoReturn, Union
)
import os
import re
import logging

import gevent.core

import minemeld.loader
from minemeld.ft import MetadataResult, NodeType

# Types
TMineMeldNodeConfig = TypedDict('TMineMeldNodeConfig', {
    'inputs': List[str],
    'prototype': str,
    'class': str,
    'config': Dict[str, Any]
}, total=False)

TMineMeldConfig = TypedDict('TMineMeldConfig', {
    'nodes': Dict[str, TMineMeldNodeConfig]
}, total=False)


LOG = logging.getLogger(__name__)
MGMTBUS_NUM_CONNS_ENV = 'MGMTBUS_NUM_CONNS'
FABRIC_NUM_CONNS_ENV = 'FABRIC_NUM_CONNS'


_ConfigChange = namedtuple(
    '_ConfigChange',
    ['nodename', 'nodeclass', 'change', 'detail']
)

_Config = namedtuple(
    '_Config',
    ['path', 'nodes', 'changes']
)


class MineMeldConfigChange(_ConfigChange):
    ADDED = 0
    DELETED = 1
    INPUT_ADDED = 2
    INPUT_DELETED = 3
    CONFIG_HUP = 4
    CONFIG_REINIT = 5

    def __new__(_cls, nodename, nodeclass, change, detail=None):
        return _ConfigChange.__new__(
            _cls,
            nodename=nodename,
            nodeclass=nodeclass,
            change=change,
            detail=detail
        )


class MineMeldConfig(_Config):
    path: str
    nodes: Dict[str, TMineMeldNodeConfig]
    changes: List[MineMeldConfigChange]

    def as_nset(self) -> Set[Tuple[str, Optional[str]]]:
        result = set()
        for nname, nvalue in self.nodes.items():
            result.add((nname, nvalue.get('class', None)))
        return result

    def compute_changes(self, oconfig: Optional['MineMeldConfig']) -> None:
        if oconfig is None:
            # oconfig is None, mark everything as added
            for nodename, nodeattrs in self.nodes.items():
                self.changes.append(
                    MineMeldConfigChange(
                        nodename=nodename, nodeclass=nodeattrs['class'], change=MineMeldConfigChange.ADDED)
                )
            return

        nodes_entrypoints = minemeld.loader.map(minemeld.loader.MM_NODES_ENTRYPOINT)

        my_nset = self.as_nset()
        other_nset = oconfig.as_nset()

        deleted = other_nset - my_nset
        added = my_nset - other_nset
        untouched = my_nset & other_nset

        # mark delted as deleted
        for nodename, nodeclass in deleted:
            change = MineMeldConfigChange(
                nodename=nodename,
                nodeclass=nodeclass,
                change=MineMeldConfigChange.DELETED,
                detail=oconfig.nodes[nodename]
            )
            self.changes.append(change)

        # mark added as added
        for nodename, nodeclass in added:
            change = MineMeldConfigChange(
                nodename=nodename,
                nodeclass=nodeclass,
                change=MineMeldConfigChange.ADDED
            )
            self.changes.append(change)

        # check inputs/output for untouched
        for nodename, nodeclass in untouched:
            my_inputs = set(self.nodes[nodename].get('inputs', []))
            other_inputs = set(oconfig.nodes[nodename].get('inputs', []))

            iadded = my_inputs - other_inputs
            ideleted = other_inputs - my_inputs

            for i in iadded:
                change = MineMeldConfigChange(
                    nodename=nodename,
                    nodeclass=nodeclass,
                    change=MineMeldConfigChange.INPUT_ADDED,
                    detail=i
                )
                self.changes.append(change)

            for i in ideleted:
                change = MineMeldConfigChange(
                    nodename=nodename,
                    nodeclass=nodeclass,
                    change=MineMeldConfigChange.INPUT_DELETED,
                    detail=i
                )
                self.changes.append(change)

            my_config = self.nodes[nodename].get('config', {})
            other_config = oconfig.nodes[nodename].get('config', {})
            if my_config == oconfig:
                continue

            node_ep = nodes_entrypoints.get(nodeclass)
            if node_ep is None:
                LOG.warning(f'No entrypoint for {nodeclass}')
                continue
            node_class = node_ep.ep.load()

            vresult = node_class.validate(my_config, other_config)
            if len(vresult.get('errors', [])) != 0:
                raise RuntimeError('Invalid config in change detection!')

            self.changes.append(
                MineMeldConfigChange(
                    nodename=nodename,
                    nodeclass=nodeclass,
                    detail=my_config,
                    change=MineMeldConfigChange.CONFIG_REINIT if vresult.get(
                        'requires_restart',
                        True) else MineMeldConfigChange.CONFIG_HUP))

    def validate(self) -> List[str]:
        result = []
        nodes = self.nodes

        for n in nodes.keys():
            if re.match(r'^[a-zA-Z0-9_\-]+$', n) is None:  # pylint:disable=W1401
                result.append('%s node name is invalid' % n)

        node_types: Dict[str, NodeType] = {}
        installed_nodes = minemeld.loader.map(minemeld.loader.MM_NODES_ENTRYPOINT)
        for n, v in nodes.items():
            nclass = v.get('class', None)
            if nclass is None:
                result.append('No class in {}'.format(n))
                continue

            mmep = installed_nodes.get(nclass, None)
            if mmep is None:
                result.append(
                    'Unknown node class {} in {}'.format(nclass, n)
                )
                continue

            if not mmep.loadable:
                result.append(
                    'Class {} in {} not safe to load'.format(nclass, n)
                )
                continue

            node_class = mmep.ep.load()
            node_types[n] = node_class.get_metadata()['node_type']
            vresult = node_class.validate(v.get('config', {}))
            for verror in vresult.get('errors', []):
                result.append(f'Invalid config for node {n}: {verror}')

        for n, v in nodes.items():
            v_inputs = v.get('inputs', [])
            if n not in node_types:
                continue

            if node_types[n] == NodeType.MINER and len(v_inputs) != 0:
                result.append(f'{n} - Miner with inputs')
                continue

            for i in v_inputs:
                if i not in nodes:
                    result.append(f'{n} -> input {i} is unknown')
                    continue

                if i not in node_types:
                    continue
                
                if node_types[i] == NodeType.OUTPUT:
                    result.append(f'{n} -> input {i} is of type {node_types[i].name}')

        if not detect_cycles(nodes):
            result.append('loop detected')

        return result

    @classmethod
    def from_dict(cls: Type['MineMeldConfig'], path: str, dconfig: Optional[TMineMeldConfig] = None) -> 'MineMeldConfig':
        if dconfig is None:
            dconfig = {}

        nodes = dconfig.get('nodes', None)
        if nodes is None:
            nodes = {}

        return cls(path=path, nodes=nodes, changes=[])


def detect_cycles(nodes: Dict[str, TMineMeldNodeConfig]) -> bool:
    # using Topoligical Sorting to detect cycles in graph, see Wikipedia
    graph: Dict[str, Dict[str, List[str]]] = {}
    S = set()
    L = []

    for n in nodes:
        graph[n] = {
            'inputs': [],
            'outputs': []
        }

    for n, v in nodes.items():
        for i in v.get('inputs', []):
            if i in graph:
                graph[i]['outputs'].append(n)
                graph[n]['inputs'].append(i)

    for gn, gv in graph.items():
        if len(gv['inputs']) == 0:
            S.add(gn)

    while len(S) != 0:
        n = S.pop()
        L.append(n)

        for m in graph[n]['outputs']:
            graph[m]['inputs'].remove(n)
            if len(graph[m]['inputs']) == 0:
                S.add(m)
        graph[n]['outputs'] = []

    nedges = 0
    for gn, gv in graph.items():
        nedges += len(gv['inputs'])
        nedges += len(gv['outputs'])

    return nedges == 0
