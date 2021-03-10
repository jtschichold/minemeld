import multiprocessing
import functools
import logging

from typing import (
    Optional, Dict, Any, List
)

import minemeld.loader

from .model import MineMeldConfig, MineMeldConfigChange


LOG = logging.getLogger(__name__)


def _destroy_node(change: MineMeldConfigChange, installed_nodes: Optional[Dict[str, Any]] = None, installed_nodes_gcs: Optional[Dict[str, Any]] = None) -> int:
    LOG.info('Destroying {!r}'.format(change))

    destroyed_name = change.nodename
    destroyed_class = change.nodeclass
    if destroyed_class is None:
        LOG.error('Node {} with no class destroyed'.format(destroyed_name))
        return 1

    # load node class GC from entry_point or from "gc" staticmethod of class
    node_gc = None
    mmep = None
    if installed_nodes_gcs is not None:
        mmep = installed_nodes_gcs.get(destroyed_class, None)
    if mmep is None:
        if installed_nodes is not None:
            mmep = installed_nodes.get(destroyed_class, None)
        if mmep is None:
            LOG.error(
                f"Could not find a entrypoint for {destroyed_name}:{destroyed_class}")
            return 1

        try:
            nodep = mmep.ep.load()

            if hasattr(nodep, 'gc'):
                node_gc = nodep.gc
        except ImportError:
            LOG.exception(
                "Error checking node class {} for gc method".format(destroyed_class))
    else:
        try:
            node_gc = mmep.ep.load()
        except ImportError:
            LOG.exception(
                "Error resolving gc for class {}".format(destroyed_class))
    if node_gc is None:
        LOG.error('Node {} with class {} with no garbage collector destroyed'.format(
            destroyed_name, destroyed_class
        ))
        return 1

    try:
        node_gc(
            destroyed_name,
            config=change.detail.get('config', None)
        )

    except:
        LOG.exception('Exception destroying old node {} of class {}'.format(
            destroyed_name, destroyed_class
        ))
        return 1

    return 0


def destroy_old_nodes(config: 'MineMeldConfig') -> None:
    # this destroys resources used by destroyed nodes
    # a nodes has been destroyed if a node with same
    # name & config does not exist in the new config
    # the case of different node config but same and name
    # and class is handled by node itself
    destroyed_nodes = config.deleted_nodes()
    LOG.info('Destroyed nodes: {!r}'.format(destroyed_nodes))
    if len(destroyed_nodes) == 0:
        return

    installed_nodes = minemeld.loader.map(minemeld.loader.MM_NODES_ENTRYPOINT)
    installed_nodes_gcs = minemeld.loader.map(
        minemeld.loader.MM_NODES_GCS_ENTRYPOINT)

    dpool: Optional[multiprocessing.pool.Pool] = multiprocessing.Pool()
    assert dpool is not None
    _bound_destroy_node = functools.partial(
        _destroy_node,
        installed_nodes=installed_nodes,
        installed_nodes_gcs=installed_nodes_gcs
    )
    dpool.imap_unordered(
        _bound_destroy_node,
        destroyed_nodes
    )
    dpool.close()
    dpool.join()
    dpool = None
