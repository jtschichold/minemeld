#  Copyright 2015-2016 Palo Alto Networks, Inc
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

"""
This module implements master and slave hub classes for MineMeld engine
management bus.

Management bus master sends commands to all managemnt bus slaves by
posting a message to a specific topic (MGMTBUS_PREFIX+'bus').
Slaves subscribe to the topic, and when a command is received they
reply back to the master by sending the answer to the queue
MGMTBUS_PREFIX+'master'. Slaves connections are multiplexed via
slave hub class.

Management bus is used to control the MineMeld engine graph and to
periodically retrieve metrics from all the nodes.
"""


import logging
import uuid
import collections
import time
import hashlib
import os
from typing import (
    Optional, Dict, Any,
    Union, cast, Callable,
    List, TypedDict, Tuple,
    TYPE_CHECKING,
)

import gevent
import gevent.event
import gevent.lock
import gevent.timeout

import redis
import json

import minemeld.comm
import minemeld.ft

if TYPE_CHECKING:
    from minemeld.chassis import Chassis
    from minemeld.ft.base import BaseFT
    from minemeld.comm.zmqredis import ZMQPubChannel
    from minemeld.config import MineMeldConfig

from .pmcollectd import PMCollectdClient
from .startupplanner import plan

LOG = logging.getLogger(__name__)

MGMTBUS_PREFIX = "mbus:"
MGMTBUS_TOPIC = MGMTBUS_PREFIX+'bus'
MGMTBUS_CHASSIS_TOPIC = MGMTBUS_PREFIX+'chassisbus'
MGMTBUS_MASTER = '@'+MGMTBUS_PREFIX+'master'
MGMTBUS_LOG_TOPIC = MGMTBUS_PREFIX+'log'
MGMTBUS_STATUS_TOPIC = MGMTBUS_PREFIX+'status'


CommRpcResult = TypedDict('CommRpcResult', {
    'answers': Dict[str,Any],
    'errors': int
}, total=False)


class MgmtbusCmdTimeout(Exception):
    pass


class MgmtbusCmdError(Exception):
    pass


class MgmtbusMaster(object):
    """MineMeld engine management bus master

    Args:
        ftlist (list): list of nodes
        config (dict): config
        comm_class (string): communication backend to be used
        comm_config (dict): config for the communication backend
    """

    def __init__(self, ftlist, config, comm_class, comm_config, num_chassis):
        super(MgmtbusMaster, self).__init__()

        self.ftlist = ftlist
        self.config = config
        self.comm_config = comm_config
        self.comm_class = comm_class
        self.num_chassis = num_chassis

        self._chassis: List[int] = []
        self._all_chassis_ready = gevent.event.Event()

        self.graph_status: Optional[str] = None

        self._start_timestamp = int(time.time())*1000
        self._status_lock = gevent.lock.Semaphore()
        self.status_glet = None
        self._status = {}

        self.SR = redis.StrictRedis.from_url(
            os.environ.get('REDIS_URL', 'unix:///var/run/redis/redis.sock')
        )

        self.comm = minemeld.comm.factory(self.comm_class, self.comm_config)
        # self._out_channel = self.comm.request_pub_channel(MGMTBUS_TOPIC)
        self.comm.request_rpc_server_channel(
            name=MGMTBUS_MASTER,
            obj=self,
            allowed_methods=['rpc_status', 'rpc_chassis_ready'],
            method_prefix='rpc_'
        )
        # self._slaves_rpc_client = self.comm.request_rpc_fanout_client_channel(
        #     MGMTBUS_TOPIC
        # )
        self._chassis_rpc_client = self.comm.request_rpc_fanout_client_channel(
            MGMTBUS_CHASSIS_TOPIC
        )
        self.comm.request_rpc_server_channel(
            name=MGMTBUS_STATUS_TOPIC,
            obj=self,
            allowed_methods=['status']
        )

    def rpc_status(self):
        """Returns collected status via RPC
        """
        return self._status

    def rpc_chassis_ready(self, chassis_id: Optional[int] = None) -> str:
        """Chassis signal ready state via this RPC
        """
        if chassis_id in self._chassis:
            LOG.error('duplicate chassis_id received in rpc_chassis_ready')
            return 'ok'
        if chassis_id is None:
            LOG.error("chassis_ready message without chassis_id")
            return 'ok'

        self._chassis.append(chassis_id)
        if len(self._chassis) == self.num_chassis:
            self._all_chassis_ready.set()

        return 'ok'

    def wait_for_chassis(self, timeout: int = 60) -> None:
        """Wait for all the chassis signal ready state
        """
        if self.num_chassis == 0:  # empty config
            return

        if not self._all_chassis_ready.wait(timeout=timeout):
            raise RuntimeError('Timeout waiting for chassis')

    def start_chassis(self) -> None:
        self._send_cmd_and_wait(
            'start',
            '<chassis>',
            timeout=60
        )

    def _send_cmd(self, command: str, target: str, params: Optional[Dict[str, Any]] = None, and_discard: bool = False) -> gevent.event.AsyncResult:
        """Sends command to slaves or chassis over mgmt bus.

        Args:
            command (str): command
            params (dict): params of the command
            and_discard (bool): discard answer, don't wait
            target (str): node name, or <nodes> for all nodes, or <chassis> for all chassis

        Returns:
            returns a gevent.event.AsyncResult that is signaled
            when all the answers are collected
        """
        if params is None:
            params = {}
        params['target'] = target

        return self._chassis_rpc_client.send_rpc(
            command,
            params=params,
            and_discard=and_discard,
            num_results=self.num_chassis
        )

    def _send_cmd_and_wait(self, command: str, target: str, params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str,Any]:
        """Simple wrapper around _send_cmd for raising exceptions
        """
        revt = self._send_cmd(command, target, params=params)
        success = revt.wait(timeout=timeout)
        if success is None:
            LOG.critical(f'Timeout in {command} to {target}')
            raise MgmtbusCmdTimeout(f'Timeout in {command} to {target}')

        result: CommRpcResult = revt.get(block=False)
        if result['errors'] > 0:
            LOG.critical(f'Errors reported in {command} to {target}')
            raise MgmtbusCmdError(f'Errors reported in {command} to {target}')

        # if target is <nodes> we flatten the answers
        ans = result.get('answers', {})
        if target == "<nodes>":
            ans = {nodename: nodeans for a in ans.values() for nodename,nodeans in a.items()}

        return ans

    def _send_node_cmd(self, nodename: str, command: str, params: Dict[str, Any] = None) -> Any:
        """Send command to a single node
        """
        if params is None:
            params = {}

        result = self._send_cmd_and_wait(
            command,
            nodename,
            params=params,
            timeout=60
        )

        return result

    def init_graph(self, config: 'MineMeldConfig') -> None:
        """Initalizes graph by sending startup messages.

        Args:
            config (MineMeldConfig): config
        """
        result = self._send_cmd_and_wait(
            'state_info', target="<nodes>", timeout=60)

        LOG.info(f'state: {result!r}')
        LOG.info(f'changes: {config.changes!r}')

        state_info = {k.split(':', 2)[-1]: v for k,
                      v in result.items()}

        startup_plan = plan(config, state_info)
        for node, command in startup_plan.items():
            LOG.info('{} <= {}'.format(node, command))
            self._send_node_cmd(node, command)

        self.graph_status = 'INIT'

    def checkpoint_graph(self, max_tries: int = 60) -> None:
        """Checkpoints the graph.

        Args:
            max_tries (int): number of minutes before giving up
        """
        LOG.info('checkpoint_graph called, checking current state')

        if self.graph_status != 'INIT':
            LOG.info(
                f'graph status {self.graph_status}, checkpoint_graph ignored')
            return

        while True:
            try:
                result = self._send_cmd_and_wait(
                    'state_info', target="<nodes>", timeout=30)

            except MgmtbusCmdTimeout:
                gevent.sleep(60)
                continue

            except MgmtbusCmdError:
                LOG.critical('errors reported from nodes in checkpoint_graph')
                gevent.sleep(60)
                continue

            all_started = True
            for answer in result.values():
                if answer.get('state', None) != minemeld.ft.ft_states.STARTED:
                    all_started = False
                    break
            if not all_started:
                LOG.error('some nodes not started yet, waiting')
                gevent.sleep(60)
                continue

            break

        chkp = str(uuid.uuid4())
        LOG.info(f'Sending checkpoint {chkp} to nodes')
        for nodename in self.ftlist:
            self._send_node_cmd(nodename, 'checkpoint', params={'value': chkp})

        ntries = 0
        while ntries < max_tries:
            try:
                result = self._send_cmd_and_wait(
                    'state_info', target="<nodes>", timeout=60)

            except (MgmtbusCmdError, MgmtbusCmdTimeout):
                LOG.error("Error retrieving nodes states after checkpoint")
                gevent.sleep(30)
                continue

            cgraphok = True
            for answer in result.values():
                cgraphok &= (answer['checkpoint'] == chkp)
            if cgraphok:
                LOG.info('checkpoint graph - all good')
                break

            gevent.sleep(2)
            ntries += 1

        if ntries == max_tries:
            LOG.error('checkpoint_graph: nodes still not in '
                      'checkpoint state after max_tries')

        self.graph_status = 'CHECKPOINT'

    def _send_collectd_metrics(self, answers):
        """Send collected metrics from nodes to collectd.

        Args:
            answers (list): list of metrics
        """

        cc = PMCollectdClient(self.SR)

        gstats = collections.defaultdict(lambda: 0)
        metrics: List[Tuple[str,int]] = []

        for source, a in answers.items():
            ntype = 'processors'
            if len(a.get('inputs', [])) == 0:
                ntype = 'miners'
            elif not a.get('output', False):
                ntype = 'outputs'

            stats = a.get('statistics', {})
            length = a.get('length', None)

            source_with_prefix = f"node:{source}"
            for m, v in stats.items():
                gstats[ntype+'.'+m] += v
                metrics.append((source_with_prefix+'.'+m, v))

            if length is not None:
                gstats['length'] += length
                gstats[ntype+'.length'] += length
                metrics.append((
                    source_with_prefix+'.length',
                    length
                ))

        for gs, v in gstats.items():
            metrics.append(('minemeld:'+gs, v))

        cc.put(metrics)

    def _merge_status(self, nodename: str, status: Dict[str, Any]) -> None:
        currstatus = self._status.get(nodename, None)
        if currstatus is not None:
            if currstatus.get('clock', -1) > status.get('clock', -2):
                LOG.error('old clock: {} > {} - dropped'.format(
                    currstatus.get('clock', -1),
                    status.get('clock', -2)
                ))
                return

        self._status[nodename] = status

        try:
            source = nodename
            self.SR.publish(
                'mm-engine-status.'+source,
                json.dumps({
                    'source': source,
                    'timestamp': int(time.time())*1000,
                    'status': status
                })
            )

        except:
            LOG.exception('Error publishing status')

    def _status_loop(self) -> None:
        """Greenlet that periodically retrieves metrics from nodes and sends
        them to collected.
        """
        loop_interval = self.config.get('STATUS_INTERVAL', '60')
        try:
            loop_interval = int(loop_interval)
        except ValueError:
            LOG.error('invalid STATUS_INTERVAL settings, '
                      'reverting to default')
            loop_interval = 60

        while True:
            try:
                result = self._send_cmd_and_wait(
                    'status', target="<nodes>", timeout=30)

            except MgmtbusCmdTimeout:
                LOG.error('timeout in waiting for status updates from nodes')
                gevent.sleep(loop_interval)
                continue

            except MgmtbusCmdError:
                gevent.sleep(loop_interval)
                continue

            with self._status_lock:
                for nodename, nodestatus in result.items():
                    self._merge_status(nodename, nodestatus)

            try:
                self._send_collectd_metrics(
                    result
                )

            except Exception:
                LOG.exception('Exception in _status_loop')

            gevent.sleep(loop_interval)

    def status(self, timestamp: int, **kwargs) -> None:
        source = kwargs.get('source', None)
        if source is None:
            LOG.error('no source in status report - dropped')
            return

        status = kwargs.get('status', None)
        if status is None:
            LOG.error('no status in status report - dropped')
            return

        if self._status_lock.locked():
            return

        with self._status_lock:
            if timestamp < self._start_timestamp:
                return

            # XXX - this prefix should be removed at some point
            self._merge_status(source, status)

    def start_status_monitor(self) -> None:
        """Starts status monitor greenlet.
        """
        if self.status_glet is not None:
            LOG.error('double call to start_status')
            return

        self.status_glet = gevent.spawn(self._status_loop)

    def stop_status_monitor(self) -> None:
        """Stops status monitor greenlet.
        """
        if self.status_glet is None:
            return
        self.status_glet.kill()
        self.status_glet = None

    def start(self) -> None:
        self.comm.start()

    def stop(self) -> None:
        self.comm.stop()


class MgmtbusSlaveHub(object):
    """Hub MineMeld engine management bus slaves. Each chassis
        has an instance of this class, and each node in the chassis
        request a channel to the management bus via this instance.

    Args:
        config (dict): config
        comm_class (string): communication backend to be used
        comm_config (dict): config for the communication backend
    """

    def __init__(self, config: str, comm_class: str, comm_config: dict) -> None:
        self.config = config
        self.comm_config = comm_config
        self.comm_class = comm_class

        self.comm = minemeld.comm.factory(self.comm_class, self.comm_config)

    def request_log_channel(self) -> 'ZMQPubChannel':
        LOG.debug("Adding log channel")
        return self.comm.request_pub_channel(  # type: ignore # XXX - cast does not work on "strings"
            topic=MGMTBUS_LOG_TOPIC,
            multi_write=True
        )

    def send_status(self, params: Dict[str, Union[str, int, bool]]) -> None:
        self.comm.send_rpc(
            dest=MGMTBUS_STATUS_TOPIC,
            method='status',
            params=params,
            block=True
        )

    def request_chassis_rpc_channel(self, chassis: 'Chassis') -> None:
        self.comm.request_rpc_server_channel(
            '{}chassis:{}'.format(MGMTBUS_PREFIX, chassis.chassis_id),
            chassis,
            allowed_methods=[
                'mgmtbus_start',
                'mgmtbus_state_info',
                'mgmtbus_initialize',
                'mgmtbus_rebuild',
                'mgmtbus_reset',
                'mgmtbus_status',
                'mgmtbus_checkpoint',
                'mgmtbus_hup',
                'mgmtbus_signal'
            ],
            method_prefix='mgmtbus_',
            fanout=MGMTBUS_CHASSIS_TOPIC
        )

    def request_channel(self, node: 'BaseFT') -> None:
        self.comm.request_rpc_server_channel(
            '{}directslave:{}'.format(MGMTBUS_PREFIX, node.name),
            node,
            allowed_methods=[
                'mgmtbus_state_info',
                'mgmtbus_initialize',
                'mgmtbus_rebuild',
                'mgmtbus_reset',
                'mgmtbus_status',
                'mgmtbus_checkpoint',
                'mgmtbus_hup',
                'mgmtbus_signal'
            ],
            method_prefix='mgmtbus_'
        )

    def add_failure_listener(self, f: Callable[[], None]) -> None:
        self.comm.add_failure_listener(f)

    def send_master_rpc(self, command: str, params: Optional[Dict[str, Union[int, bool, str]]] = None, timeout: Optional[int] = None) -> Optional[Any]:
        return self.comm.send_rpc(
            MGMTBUS_MASTER,
            command,
            params,
            timeout=timeout
        )

    def start(self) -> None:
        LOG.debug('mgmtbus start called')
        self.comm.start()

    def stop(self) -> None:
        self.comm.stop()


def master_factory(config, comm_class, comm_config, nodes, num_chassis):
    """Factory of management bus master instances

    Args:
        config (dict): management bus master config
        comm_class (string): communication backend.
            Unused, ZMQRedis is always used
        comm_config (dict): config of the communication backend
        fts (list): list of nodes

    Returns:
        Instance of minemeld.mgmtbus.MgmtbusMaster class
    """
    _ = comm_class  # noqa

    return MgmtbusMaster(
        ftlist=nodes,
        config=config,
        comm_class='ZMQRedis',
        comm_config=comm_config,
        num_chassis=num_chassis
    )


def slave_hub_factory(config, comm_class: str, comm_config: dict) -> MgmtbusSlaveHub:
    """Factory of management bus slave hub instances

    Args:
        config (dict): management bus master config
        comm_class (string): communication backend.
            Unused, ZMQRedis is always used
        comm_config (dict): config of the communication backend.

    Returns:
        Instance of minemeld.mgmtbus.MgmtbusSlaveHub class
    """
    _ = comm_class  # noqa

    return MgmtbusSlaveHub(
        config,
        'ZMQRedis',
        comm_config
    )
