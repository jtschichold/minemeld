import time
import os
import logging
import uuid
import json
import collections
from typing import (
    List, Optional, TypedDict, Dict, Tuple, Any
)

import gevent.lock
import gevent.event
import redis
import zmq.green as zmq

import minemeld.startupplanner
from minemeld.config import MineMeldConfig
from minemeld.comm.rpc import (
    Reactor as RPCReactor,
    AsyncClient as RPCAsyncClient,
    Dispatcher as RPCDispatcher,
    RPCRequestParams
)
from minemeld.ft import ft_states
from minemeld.pmcollectd import PMCollectdClient
from minemeld.defaults import (
    DEFAULT_REDIS_URL, DEFAULT_MINEMELD_COMM_PATH,
    DEFAULT_MINEMELD_STATUS_INTERVAL
)


LOG = logging.getLogger(__name__)


class OrchestratorCmdError(Exception):
    pass


class OrchestratorCmdTimeout(Exception):
    pass


class Orchestrator:
    def __init__(self, chassis_plan: List[List[str]]):
        self.chassis_plan = chassis_plan

        self._chassis: List[int] = []
        self._all_chassis_ready = gevent.event.Event()

        self.graph_status: Optional[str] = None

        self._start_timestamp = int(time.time())*1000
        self._status_lock = gevent.lock.Semaphore()
        self.status_glet: Optional[gevent.Greenlet] = None
        self._status: Dict[str, dict] = {}

        self.redis_client: redis.Redis = redis.Redis.from_url(
            os.environ.get('MINEMELD_REDIS_URL', DEFAULT_REDIS_URL)
        )

        self.context: zmq.Context = zmq.Context()
        self.rpc_reactor = RPCReactor(
            path=os.environ.get('MINEMELD_COMM_PATH', DEFAULT_MINEMELD_COMM_PATH),
            context=self.context
        )

        self.routing_table: Dict[str, str] = {}
        for cn, node_list in enumerate(chassis_plan):
            chassis_endpoint = f'chassis:{cn}'
            for n in node_list:
                self.routing_table[n] = chassis_endpoint

        self.rpc_reactor.new_router(
            name='orchestrator',
            handler=self.on_rpc_request,
            routing_table=self.routing_table
        )
        self.rpc_async_client: RPCAsyncClient = self.rpc_reactor.new_client(
            name='orchestrator'
        )
        self.rpc_reactor_glet: Optional[gevent.Greenlet] = None
        self.rpc_polling_glet: Optional[gevent.Greenlet] = None

    def on_rpc_request(self, method: str, **kwargs) -> gevent.event.AsyncResult:
        result = gevent.event.AsyncResult()

        if method == 'status':
            result.set(
                value=self._status
            )

        elif method == 'chassis_ready':
            result.set(
                value=self.chassis_ready(**kwargs)
            )

        elif method == 'status':
            result.set(
                value=self.status(**kwargs)
            )

        else:
            LOG.error(f'Orchestrator - RPC request with unknown method {method}')
            result.set_exception(
                RuntimeError(f'Orchestrator - RPC call for unknown method {method}')
            )

        return result

    def chassis_ready(self, chassis_id: Optional[int] = None) -> str:
        """Chassis signal ready state via this RPC
        """
        if chassis_id in self._chassis:
            LOG.error('duplicate chassis_id received in rpc_chassis_ready')
            return 'ok'
        if chassis_id is None:
            LOG.error("chassis_ready message without chassis_id")
            return 'ok'

        self._chassis.append(chassis_id)
        if len(self._chassis) == len(self.chassis_plan):
            self._all_chassis_ready.set()

        return 'ok'

    def status(self, timestamp: int, **kwargs) -> str:
        source = kwargs.get('source', None)
        if source is None:
            LOG.error('no source in status report - dropped')
            return 'Ignored'

        status = kwargs.get('status', None)
        if status is None:
            LOG.error('no status in status report - dropped')
            return 'Ignored'

        if self._status_lock.locked():
            return 'Ignored'

        with self._status_lock:
            if timestamp < self._start_timestamp:
                return 'Ignored'

            # XXX - this prefix should be removed at some point
            self._merge_status(source, status)

        return "OK"

    def wait_for_chassis(self, timeout: int = 60) -> None:
        """Wait for all the chassis signal ready state
        """
        if len(self.chassis_plan) == 0:  # empty config
            return

        if not self._all_chassis_ready.wait(timeout=timeout):
            raise RuntimeError('Timeout waiting for chassis')

    def start_chassis(self) -> None:
        async_results = [
            self.rpc_async_client.send_rpc(
                remote=f'chassis:{cn}',
                method='start',
                node='<chassis>'
            )
            for cn in range(len(self.chassis_plan))
        ]
        wresult = gevent.wait(async_results, timeout=60.0)
        if len(wresult) != len(async_results):
            raise gevent.Timeout()

    def get_state_info(self, timeout: float = 60.0) -> Dict[str, dict]:
        async_results = [
            self.rpc_async_client.send_rpc(
                remote=f'chassis:{cn}',
                method='state_info',
                node='<chassis>'
            )
            for cn in range(len(self.chassis_plan))
        ]

        try:
            wresult = gevent.wait(async_results, timeout=timeout)
            if len(wresult) != len(async_results):
                raise OrchestratorCmdTimeout()

        except gevent.Timeout:
            raise OrchestratorCmdTimeout()

        result: Dict[str, dict] = {}
        for ar in async_results:
            assert ar is not None
            ans = ar.get(block=False)
            LOG.debug(f'Answer: {ans}')
            if ans.get('error', None) is not None:
                raise OrchestratorCmdError(f'Error in state_info: {ans["error"]}')

            result.update(ans['answer'])

        return result

    def get_status(self, timeout: float = 30.0) -> Dict[str, dict]:
        async_results = [
            self.rpc_async_client.send_rpc(
                remote=f'chassis:{cn}',
                method='status',
                node='<chassis>'
            )
            for cn in range(len(self.chassis_plan))
        ]

        try:
            wresult = gevent.wait(async_results, timeout=timeout)
            if len(wresult) != len(async_results):
                raise OrchestratorCmdTimeout()

        except gevent.Timeout:
            raise OrchestratorCmdTimeout()

        result: Dict[str, dict] = {}
        for ar in async_results:
            assert ar is not None
            ans = ar.get(block=False)
            if ans.get('error', None) is not None:
                raise OrchestratorCmdError(f'Error in status: {ans["error"]}')

            result.update(ans['answer'])

        return result

    def checkpoint_chassis(self, checkpoint=str) -> None:
        for cn in range(len(self.chassis_plan)):
            self.rpc_async_client.send_rpc(
                remote=f'chassis:{cn}',
                method='checkpoint_nodes',
                node='<chassis>',
                params={
                    'value': checkpoint
                },
                ignore_answer=True
            )

    def init_graph(self, config: 'MineMeldConfig') -> None:
        """Initalizes graph by sending startup messages.

        Args:
            config (MineMeldConfig): config
        """
        state_info = self.get_state_info()

        LOG.info(f'state: {state_info!r}')
        LOG.info(f'changes: {config.changes!r}')

        startup_plan = minemeld.startupplanner.plan(config, state_info)
        plans_per_chassis: Dict[str, Dict[str, Dict[str,str]]] = collections.defaultdict(dict)
        for node, command in startup_plan.items():
            remote = self.routing_table[node]
            plans_per_chassis[remote][node] = dict(command=command)

        async_results: List[gevent.event.AsyncResult] = [] 
        for remote, chassis_plan in plans_per_chassis.items():
            ar = self.rpc_async_client.send_rpc(
                remote=remote,
                method='init_nodes',
                params=chassis_plan,
                node='<chassis>'
            )
            assert ar is not None
            async_results.append(ar)
        wresult = gevent.wait(async_results, timeout=60.0)
        if len(wresult) != len(async_results):
            raise gevent.Timeout()

        LOG.debug(f'Orchestrator: init_nodes results: {[ar.get() for ar in async_results]}')

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
                state_info = self.get_state_info(timeout=30.0)

            except OrchestratorCmdTimeout:
                gevent.sleep(60)
                continue

            except OrchestratorCmdError:
                LOG.critical('errors reported from nodes in checkpoint_graph')
                gevent.sleep(60)
                continue

            all_started = next((False for a in state_info.values() if a.get('state', None) != ft_states.STARTED), True)
            if not all_started:
                LOG.error('some nodes not started yet, waiting')
                gevent.sleep(60)
                continue

            break

        chkp = str(uuid.uuid4())
        LOG.info(f'Sending checkpoint {chkp} to nodes')
        self.checkpoint_chassis(checkpoint=chkp)

        ntries = 0
        while ntries < max_tries:
            try:
                state_info = self.get_state_info(timeout=60.0)

            except (OrchestratorCmdError, OrchestratorCmdTimeout):
                LOG.error("Error retrieving nodes states after checkpoint")
                gevent.sleep(30)
                continue

            cgraphok = True
            for answer in state_info.values():
                cgraphok &= (answer['checkpoint'] == chkp)
            if cgraphok:
                LOG.info('checkpoint graph - all good')
                break

            LOG.debug('Orchestrator - checkpoint did not happen yet, waiting')
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

        cc = PMCollectdClient(self.redis_client)

        gstats = collections.defaultdict(int)
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
            self.redis_client.publish(
                'mm-engine-status.'+source,
                json.dumps({
                    'source': source,
                    'timestamp': int(time.time())*1000,
                    'status': status
                })
            )

        except Exception as e:
            LOG.error(f'Error publishing status: {str(e)}')

    def _status_loop(self) -> None:
        """Greenlet that periodically retrieves metrics from nodes and sends
        them to collected.
        """
        try:
            loop_interval = int(os.environ.get('MINEMELD_STATUS_INTERVAL', DEFAULT_MINEMELD_STATUS_INTERVAL))
        except ValueError:
            LOG.error('invalid STATUS_INTERVAL settings, '
                      'reverting to default')
            loop_interval = 60

        if loop_interval == 0:
            return

        while True:
            try:
                result = self.get_status(timeout=30.0)
                LOG.info(f'Orchestrator - status: {json.dumps(result)}')

            except OrchestratorCmdTimeout:
                LOG.error('timeout in waiting for status updates from nodes')
                gevent.sleep(loop_interval)
                continue

            except OrchestratorCmdError:
                gevent.sleep(loop_interval)
                continue

            with self._status_lock:
                for nodename, nodestatus in result.items():
                    self._merge_status(nodename, nodestatus)

            try:
                self._send_collectd_metrics(
                    result
                )

            except Exception as e:
                LOG.error(f'Exception in _status_loop: {str(e)}')

            gevent.sleep(loop_interval)

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

    def _rpc_reactor_loop(self):
        if self.rpc_polling_glet is None:
            self.rpc_polling_glet = gevent.spawn(self.rpc_reactor.select)

        while True:
            LOG.debug(f'Orchestrator getting async results')
            ars = self.rpc_reactor.get_async_results()
            ars.append(self.rpc_polling_glet)

            self.rpc_reactor.new_async_result.clear()
            ars.append(self.rpc_reactor.new_async_result)

            LOG.debug(f'Orchestrator waiting')
            try:
                gevent.wait(ars, timeout=None, count=1)

            except gevent.Timeout:
                pass

            LOG.debug(f'Orchestrator waiting done')

            self.rpc_reactor.dispatch_async_results()
            LOG.debug(f'Orchestrator dispatching async results done')

            if not self.rpc_polling_glet.ready():
                continue

            LOG.debug(f'Orchestrator - RPC polling ready')
            dispatchers: List[RPCDispatcher] = self.rpc_polling_glet.get()
            LOG.debug(f'Orchestrator - running RPC dispatchers')
            for d in dispatchers:
                while d.run():
                    pass
            self.rpc_polling_glet = gevent.spawn(self.rpc_reactor.select)

    def on_rpc_reactor_glet_exit(self, g: gevent.Greenlet) -> None:
        LOG.error('_ioloop_failure')

        try:
            g.get()

        except gevent.GreenletExit:
            return

        except:
            LOG.critical("Orchestrator RPC Reactor failure: exception in ioloop", exc_info=True)

    def start(self) -> None:
        self.rpc_reactor.connect()
        self.rpc_reactor_glet = gevent.spawn(self._rpc_reactor_loop)
        self.rpc_reactor_glet.link(
            self.on_rpc_reactor_glet_exit
        )

    def stop(self) -> None:
        if self.rpc_reactor_glet is not None:
            self.rpc_reactor_glet.kill()
            self.rpc_reactor_glet = None

        if self.rpc_polling_glet is not None:
            self.rpc_polling_glet.kill()
            self.rpc_polling_glet = None

        self.rpc_reactor.disconnect()
