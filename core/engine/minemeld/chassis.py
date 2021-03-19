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
    ChassisNode, factory as node_factory, ft_states
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
            nodevalue = self.config[nodename]
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
            topics=pubsub_topics
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
                    subscriber_number=topic_subscribers[i].index(i),
                    topic=i,
                    handler=new_fts[ftname].on_message
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
        self.rpc_client.send_rpc(
            remote='orchestrator',
            method='chassis_ready',
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
                    params=params
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

        aresults: List[RPCNodeAsyncAnswer] = []
        for nodename, nodeinstance in self.fts.items():
            nodeargs = args.get(nodename, {})
            nodeanswer = nodeinstance.on_msg(
                method=method,
                **nodeargs
            )
            assert nodeanswer is not None
            aresults.append(nodeanswer)
        
        try:
            gevent.wait(aresults, timeout=timeout)

        except gevent.greenlet.GreenletExit:
            return

        except gevent.Timeout:
            result.set(error='Timeout in request')

        aggregated_results: RPCAggregatedAnswer = {
            'answer': {},
            'error': None
        }
        for ar in aresults:
            try:
                node_result = ar.get()
                if node_result.get('error', None) is not None:
                    result.set(error=node_result.get('error'))
                    return

                aggregated_results.update(cast(Any, node_result))  # XXX - mypy error

            except Exception as e:
                result.set(error=str(e))
                return

        result.set(answer=aggregated_results['answer'], error=aggregated_results['error'])

    def on_rpc_request(self, method: str, node: str, **kwargs) -> gevent.event.AsyncResult:
        result = gevent.event.AsyncResult()

        if method == 'start':
            assert node == '<chassis>'
            self.start()
            result.set(value='OK')

        elif method == 'state_info':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='state_info', timeout=20.0)

        elif method == 'status':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='status', timeout=20.0)

        elif method == 'checkpoint_nodes':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='checkpoint', timeout=40.0, args=kwargs)

        elif method == 'init_nodes':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='init', args=kwargs)

        elif method == 'configure':
            assert node == '<chassis>'
            gevent.spawn(self.async_nodes_request, result=result, method='configure', args=kwargs)

        elif method == 'signal':
            if node not in self.fts:
                result.set_exception(f'chassis:{self.chassis_id} - Unknown node: {node}')
            return self.fts[node].on_msg(method='signal', **kwargs)

        else:
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
        while not self.pubsub_dispatch.is_set():
            dispatchers = self.rpc_reactor.select(0.5)
            for d in dispatchers:
                d.run()
        
        if self.reactor_polling_glet is None:
            self.reactor_polling_glet = gevent.spawn(self.rpc_reactor.select)

        while True:
            ars = self.rpc_reactor.get_async_results()
            ars.extend(self.reactor_polling_glet)
            gevent.wait(ars, timeout=self.pubsub_reactor.wait_interval())

            # first RPCs
            self.rpc_reactor.dispatch_async_results()
            if self.reactor_polling_glet.ready():
                dispatchers: List[RPCDispatcher] = self.reactor_polling_glet.get()
                for d in dispatchers:
                    d.run()
                self.reactor_polling_glet = gevent.spawn(self.rpc_reactor.select)

            self.pubsub_reactor.dispatch()
            
    def fts_init(self) -> bool:
        for ft in self.fts.values():
            if ft.state < ft_states.INIT:
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

        self.log_glet = gevent.spawn(self._log_actor)
        self.status_glet = gevent.spawn(self._status_actor)

        for ftname, ft in self.fts.items():
            LOG.debug("starting %s", ftname)
            ft.start()

        self.pubsub_dispatch.set()
