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
minemeld.chassis

A chassis instance contains a list of nodes and a fabric.
Nodes communicate using the fabric.
"""

import os
import logging
from typing import (
    Dict, Optional, List,
    Any, TYPE_CHECKING, Tuple,
    TypedDict, cast
)
from collections import defaultdict

import gevent
import gevent.queue
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)

import zmq.green as zmq

from minemeld.ft import (
    ChassisNode, factory as node_factory, FTState
)
from minemeld.config import MineMeldConfig
from minemeld.comm.rpc import (
    Server as RPCServer,
    Reactor as RPCReactor
)
from minemeld.comm.pubsub import (
    Subscriber, Publisher,
    Reactor as PubSubReactor
)
from minemeld.comm.rpc import (
    AsyncClient as RPCAsyncClient,
    Dispatcher as RPCDispatcher,
    Server as RPCServer,
    Reactor as RPCReactor
)
from minemeld.defaults import DEFAULT_MINEMELD_COMM_PATH


LOG = logging.getLogger(__name__)


class RPCAggregatedAnswer(TypedDict):
    answer: Optional[Dict[str,Any]]
    error: Optional[str]


class RPCNodeAsyncAnswer(gevent.event.AsyncResult):
    def __init__(self, answer: Optional[Dict[str,Any]] = None, error: Optional[str] = None):
        super().__init__()
        if answer is not None or error is not None:
            self.set(answer=answer, error=error)

    def set(self, answer: Optional[Dict[str,Any]] = None, error: Optional[str] = None) -> None:
        super().set(value={
            'answer': answer,
            'error': error
        })

    def get(self, block: bool = True, timeout: Optional[float] = None) -> RPCAggregatedAnswer:
        return super().get(block=block, timeout=timeout)


class Chassis:
    def __init__(self, chassis_id: int, config: MineMeldConfig, chassis_plan: List[List[str]]) -> None:
        self.chassis_id = chassis_id
        self.chassis_pid = os.getpid()
        self.chassis_plan = chassis_plan
        self.config = config

        self.fts: Dict[str, ChassisNode] = {}
        self.poweroff = gevent.event.AsyncResult()

        self.log_channel_queue = gevent.queue.Queue(maxsize=128)
        self.log_channel: Optional[Any] = None # XXX - TBD
        self.log_glet: Optional[gevent.Greenlet] = None

        self.status_channel_queue = gevent.queue.Queue(maxsize=128)
        self.status_glet: Optional[gevent.Greenlet] = None

        self.pubsub_subscribers: List[Subscriber] = []
        self.pubsub_publishers: List[PubSubReactor] = []
        self.pubsub_reactor: Optional[PubSubReactor] = None
        self.pubsub_dispatch: gevent.event.Event = gevent.event.Event()

        self.context: zmq.Context = zmq.Context()
        self.rpc_client: Optional[RPCAsyncClient] = None
        self.rpc_server: Optional[RPCServer] = None
        self.rpc_reactor: Optional[RPCReactor] = None

        self.reactor_glet: Optional[gevent.Greenlet] = None
        self.reactor_polling_glet: Optional[gevent.Greenlet] = None

    def initialize(self):
        # list of pubsub topics to be loaded by the Reactor
        pubsub_topics: Dict[str,Optional[int]] = {}
        # list of subscribers per each topic
        topic_subscribers: Dict[str,List[str]] = defaultdict(list)

        for nodename in sorted(self.config.nodes.keys()):
            nodevalue = self.config.nodes[nodename]
            inputs: List[str] = nodevalue.get('inputs')
            for i in inputs:
                topic_subscribers[i].append(nodename)
                
            if nodename in self.chassis_plan[self.chassis_id]:
                for i in inputs:
                    pubsub_topics[i] = None
        for nodename in self.chassis_plan[self.chassis_id]:
            if len(topic_subscribers[nodename]) != 0:
                pubsub_topics[nodename] = len(topic_subscribers[nodename])

        # create pubsub the reactor
        self.pubsub_reactor = PubSubReactor(
            path=os.environ.get('MINEMELD_COMM_PATH', DEFAULT_MINEMELD_COMM_PATH),
            topics=list(pubsub_topics.items())
        )

        # rpc
        self.rpc_reactor = RPCReactor(
            path=os.environ.get('MINEMELD_COMM_PATH', DEFAULT_MINEMELD_COMM_PATH),
            context=self.context
        )
        self.rpc_server = self.rpc_reactor.new_server(
            name=f'chassis:{self.chassis_id}',
            handler=self.on_rpc_request
        )
        self.rpc_client = self.rpc_reactor.new_client(
            name=f'chassis:{self.chassis_id}'
        )

        # create nodes
        new_fts: Dict[str, ChassisNode] = {}
        for ftname in self.chassis_plan[self.chassis_id]:
            ftconfig = self.config.nodes[ftname]
            new_fts[ftname] = node_factory(
                ftconfig['class'],
                name=ftname,
                chassis=self,
                num_inputs=len(ftconfig.get('inputs', []))
            )

            if len(topic_subscribers[ftname]) != 0:
                p = self.pubsub_reactor.new_publisher(
                    ftname
                )
                new_fts[ftname].connect(p)

            for i in ftconfig.get('inputs', []):
                self.pubsub_subscribers.append(self.pubsub_reactor.new_subscriber(
                    subscriber_number=topic_subscribers[i].index(ftname),
                    topic=i,
                    handler=new_fts[ftname].on_pubsub_reactor_msg
                ))

        # configure nodes
        self.fts = new_fts
        for ftname in self.chassis_plan[self.chassis_id]:
            new_config = self.config.nodes[ftname].get('config', {})
            self.fts[ftname].configure(new_config)
            self.fts[ftname].start_dispatch()

        self.rpc_reactor.connect()
        self.reactor_glet = gevent.spawn(self._reactor_loop)
        self.reactor_glet.link(self.on_reactor_glet_exit)

        # chassis_ready
        LOG.info(f'Chassis:{self.chassis_id} ready')
        self.rpc_client.send_rpc(
            remote='orchestrator',
            method='chassis_ready',
            params={
                'chassis_id': self.chassis_id
            },
            ignore_answer=True
        )

    def _log_actor(self) -> None:
        if self.log_channel is None:
            return

        while True:
            try:
                params = self.log_channel_queue.get()
                self.log_channel.publish(
                    method='log',
                    params=params
                )

            except Exception:
                LOG.exception('Error sending log')

    def log(self, timestamp: int, nodename: str, log_type: str, value: dict) -> None:
        if self.log_channel is None:
            return

        self.log_channel_queue.put({
            'timestamp': timestamp,
            'source': nodename,
            'log_type': log_type,
            'log': value
        })

    def _status_actor(self) -> None:
        assert self.rpc_client is not None

        while True:
            try:
                params = self.status_channel_queue.get()
                self.rpc_client.send_rpc(
                    remote='orchestrator',
                    method='status',
                    params=params,
                    ignore_answer=True
                )

            except gevent.GreenletExit:
                return

            except Exception:
                LOG.exception('Error publishing status')

    def publish_status(self, timestamp: int, nodename: str, status: dict) -> None:
        self.status_channel_queue.put({
            'timestamp': timestamp,
            'source': nodename,
            'status': status
        })

    def async_nodes_request(self, result: RPCNodeAsyncAnswer, method: 'str', timeout: Optional[float] = None, args: Optional[Dict[str, dict]] = None) -> None:
        if args is None:
            args = {}

        aresults: Dict[str, gevent.event.AsyncResult] = {}
        for nodename, nodeinstance in self.fts.items():
            nodeargs = args.get(nodename, {})
            nodeanswer = nodeinstance.on_rpc_reactor_msg(
                method=method,
                **nodeargs
            )
            assert nodeanswer is not None
            aresults[nodename] = nodeanswer
        
        try:
            LOG.debug(f'Chassis:{self.chassis_id} - waiting for {len(aresults)} answers to {method} for {timeout}')
            wresult = gevent.wait(aresults.values(), timeout=timeout)
            if len(wresult) != len(aresults):
                raise gevent.Timeout()

        except gevent.greenlet.GreenletExit:
            return

        except gevent.Timeout:
            result.set(error='Timeout in request')
            return

        LOG.debug(f'Chassis:{self.chassis_id} - waiting for {len(aresults)} answers to {method} done')

        aggregated_results: Dict[str, Any] = {}
        for nodename, aranswer in aresults.items():
            try:
                node_result = aranswer.get()

                aggregated_results[nodename] = node_result

            except Exception as e:
                result.set(error=str(e))
                return

        LOG.debug(f'Chassis:{self.chassis_id} - setting {result}')
        result.set(answer=aggregated_results)

    def on_rpc_request(self, method: str, node: str, **kwargs) -> gevent.event.AsyncResult:
        result = RPCNodeAsyncAnswer()

        LOG.debug(f'Chassis:{self.chassis_id} recvd {node}/{method}/{kwargs}')

        if method == 'start':
            assert node == '<chassis>'
            self.start()
            result.set(answer={'<chassis>': 'OK'})

        elif method == 'state_info':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='state_info', timeout=20.0)

        elif method == 'status':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='status', timeout=20.0)

        elif method == 'checkpoint_nodes':
            assert node == '<chassis>'
            args = {nodename: kwargs for nodename in self.chassis_plan[self.chassis_id]}
            gevent.spawn(self.async_nodes_request, result=result, method='checkpoint', timeout=40.0, args=args)

        elif method == 'init_nodes':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='init', args=kwargs)

        elif method == 'configure':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='configure', args=kwargs)

        elif method == 'signal':
            if node not in self.fts:
                result.set_exception(f'chassis:{self.chassis_id} - Unknown node: {node}')
            return self.fts[node].on_rpc_reactor_msg(method='signal', **kwargs)

        else:
            LOG.error(f'Chassis:{self.chassis_id} - RPC request received for unknown method: {method}')
            result.set_exception(
                RuntimeError(f'Chassis:{self.chassis_id} - RPC call for unknown method {method}')
            )

        return result

    def on_reactor_glet_exit(self, g: gevent.Greenlet) -> None:
        try:
            g.get()

        except gevent.GreenletExit:
            return

        except:
            LOG.critical(f"Chassis:{self.chassis_id} Reactor failure: exception in ioloop", exc_info=True)
            self.stop()

    def _reactor_loop(self):
        if self.reactor_polling_glet is None:
            self.reactor_polling_glet = gevent.spawn(self.rpc_reactor.select)

        while True:
            ars = self.rpc_reactor.get_async_results()
            ars.append(self.reactor_polling_glet)

            self.rpc_reactor.new_async_result.clear()
            ars.append(self.rpc_reactor.new_async_result)

            if not self.pubsub_dispatch.is_set():
                ars.append(self.pubsub_dispatch)

            timeout = self.pubsub_reactor.wait_interval() if self.pubsub_dispatch.is_set() else None
            LOG.debug(f'Chassis:{self.chassis_id} - waiting on {ars}')
            LOG.debug(f'Chassis:{self.chassis_id} - waiting for {timeout} secs')

            try:
                wresult = gevent.wait(ars, timeout=timeout, count=1)

            except gevent.Timeout:
                pass

            LOG.debug(f'Chassis:{self.chassis_id} - waiting done: {wresult}')

            # first RPCs
            LOG.debug(f'Chassis:{self.chassis_id} - dispatching async results')
            self.rpc_reactor.dispatch_async_results()
            LOG.debug(f'Chassis:{self.chassis_id} - running RPC dispatchers')
            if self.reactor_polling_glet.ready():
                dispatchers: List[RPCDispatcher] = self.reactor_polling_glet.get()
                for d in dispatchers:
                    while d.run():
                        pass
                self.reactor_polling_glet = gevent.spawn(self.rpc_reactor.select)

            if self.pubsub_dispatch.is_set():
                LOG.debug(f'Chassis:{self.chassis_id} - running PubSub dispatch')
                self.pubsub_reactor.dispatch()

    def fts_init(self) -> bool:
        for ft in self.fts.values():
            if ft.state.value < FTState.INIT.value:
                return False
        return True

    def on_sig_stop(self, signum: int, sigstack: Any) -> None:
        gevent.spawn(self.stop)

    def stop(self) -> None:
        LOG.info("chassis stop called")

        if self.log_glet is not None:
            self.log_glet.kill()
            self.log_glet = None

        if self.status_glet is not None:
            self.status_glet.kill()
            self.status_glet = None

        if self.reactor_glet is not None:
            self.reactor_glet.kill()
            self.reactor_glet = None

        for ftname, ft in self.fts.items():
            try:
                ft.stop()
            except Exception:
                LOG.exception(f'Error stopping {ftname}')

        if self.rpc_reactor is not None:
            self.rpc_reactor.disconnect()
        if self.pubsub_reactor is not None:
            self.pubsub_reactor.disconnect()

        LOG.info('chassis - stopped')
        self.poweroff.set(value='stop')

    def start(self) -> None:
        LOG.info("chassis start called")
        assert self.pubsub_reactor is not None

        self.log_glet = gevent.spawn(self._log_actor)
        self.status_glet = gevent.spawn(self._status_actor)

        for ftname, ft in self.fts.items():
            LOG.debug("starting %s", ftname)
            ft.start()

        self.pubsub_reactor.connect()
        self.pubsub_dispatch.set()
