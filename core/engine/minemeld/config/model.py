from collections import namedtuple
from typing import (
    TypedDict, List, Any,
    Dict, Set, Optional,
    Type, Tuple, TextIO,
    NoReturn, Union
)
import os
import re

import gevent.core

import minemeld.loader

# Types
TMineMeldNodeConfig = TypedDict('TMineMeldNodeConfig', {
    'inputs': List[str],
    'output': bool,
    'prototype': str,
    'class': str,
    'config': Dict[str, Any]
}, total=False)

TMineMeldConfig = TypedDict('TMineMeldConfig', {
    'fabric': Dict,
    'mgmtbus': Dict,
    'nodes': Dict[str, TMineMeldNodeConfig]
}, total=False)


MGMTBUS_NUM_CONNS_ENV = 'MGMTBUS_NUM_CONNS'
FABRIC_NUM_CONNS_ENV = 'FABRIC_NUM_CONNS'


CHANGE_ADDED = 0
CHANGE_DELETED = 1
CHANGE_INPUT_ADDED = 2
CHANGE_INPUT_DELETED = 3
CHANGE_OUTPUT_ENABLED = 4
CHANGE_OUTPUT_DISABLED = 5

_ConfigChange = namedtuple(
    '_ConfigChange',
    ['nodename', 'nodeclass', 'change', 'detail']
)

_Config = namedtuple(
    '_Config',
    ['nodes', 'fabric', 'mgmtbus', 'changes']
)


class MineMeldConfigChange(_ConfigChange):
    def __new__(_cls, nodename, nodeclass, change, detail=None):
        return _ConfigChange.__new__(
            _cls,
            nodename=nodename,
            nodeclass=nodeclass,
            change=change,
            detail=detail
        )


class MineMeldConfig(_Config):
    nodes: Dict[str, TMineMeldNodeConfig]
    fabric: Dict
    mgmtbus: Dict
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
                        nodename=nodename, nodeclass=nodeattrs['class'], change=CHANGE_ADDED)
                )
            return

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
                change=CHANGE_DELETED,
                detail=oconfig.nodes[nodename]
            )
            self.changes.append(change)

        # mark added as added
        for nodename, nodeclass in added:
            change = MineMeldConfigChange(
                nodename=nodename,
                nodeclass=nodeclass,
                change=CHANGE_ADDED
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
                    change=CHANGE_INPUT_ADDED,
                    detail=i
                )
                self.changes.append(change)

            for i in ideleted:
                change = MineMeldConfigChange(
                    nodename=nodename,
                    nodeclass=nodeclass,
                    change=CHANGE_INPUT_DELETED,
                    detail=i
                )
                self.changes.append(change)

            my_output = self.nodes[nodename].get('output', False)
            other_output = oconfig.nodes[nodename].get('output', False)

            if my_output == other_output:
                continue

            change_type = CHANGE_OUTPUT_DISABLED
            if my_output:
                change_type = CHANGE_OUTPUT_ENABLED

            change = MineMeldConfigChange(
                nodename=nodename,
                nodeclass=nodeclass,
                change=change_type
            )
            self.changes.append(change)

    def deleted_nodes(self) -> List[MineMeldConfigChange]:
        return [c for c in self.changes if c.change == CHANGE_DELETED]

    def validate(self) -> List[str]:
        result = []
        nodes = self.nodes

        for n in nodes.keys():
            if re.match('^[a-zA-Z0-9_\-]+$', n) is None:  # pylint:disable=W1401
                result.append('%s node name is invalid' % n)

        for n, v in nodes.items():
            for i in v.get('inputs', []):
                if i not in nodes:
                    result.append('%s -> %s is unknown' % (n, i))
                    continue

                if not nodes[i].get('output', False):
                    result.append('%s -> %s output disabled' %
                                (n, i))

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

        if not detect_cycles(nodes):
            result.append('loop detected')

        return result

    @classmethod
    def from_dict(cls: Type['MineMeldConfig'], dconfig: Optional[TMineMeldConfig] = None) -> 'MineMeldConfig':
        if dconfig is None:
            dconfig = {}

        fabric = dconfig.get('fabric', None)
        if fabric is None:
            fabric_num_conns = int(
                os.getenv(FABRIC_NUM_CONNS_ENV, 50)
            )

            fabric = {
                'class': 'ZMQRedis',
                'config': {
                    'num_connections': fabric_num_conns,
                    'priority': gevent.core.MINPRI  # pylint:disable=E1101
                }
            }

        mgmtbus = dconfig.get('mgmtbus', None)
        if mgmtbus is None:
            mgmtbus_num_conns = int(
                os.getenv(MGMTBUS_NUM_CONNS_ENV, 10)
            )

            mgmtbus = {
                'transport': {
                    'class': 'ZMQRedis',
                    'config': {
                        'num_connections': mgmtbus_num_conns,
                        'priority': gevent.core.MAXPRI  # pylint:disable=E1101
                    }
                },
                'master': {},
                'slave': {}
            }

        nodes = dconfig.get('nodes', None)
        if nodes is None:
            nodes = {}

        return cls(nodes=nodes, fabric=fabric, mgmtbus=mgmtbus, changes=[])


def detect_cycles(nodes: Dict[str,TMineMeldNodeConfig]) -> bool:
    # using Topoligical Sorting to detect cycles in graph, see Wikipedia
    graph: Dict[str,Dict[str,List[str]]] = {}
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