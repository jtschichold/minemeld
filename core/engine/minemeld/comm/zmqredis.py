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

# disable import error
# pylint:disable=E1101

"""
This module implements ZMQ and Redis communication class for mgmtbus and fabric.
"""


import logging
import uuid
import os
import time
from typing import (
    Any, Optional, List,
    Dict, Union, Callable,
    TYPE_CHECKING,
)

import gevent
import gevent.event
import ujson as json
from errno import EAGAIN

import redis
import zmq.green as zmq

LOG = logging.getLogger(__name__)


def _bytes_serializer(o: Any) -> str:
    raise TypeError(f"ZMQRedis: {o} not JSON serializable")


class RedisPubChannel(object):
    def __init__(self, topic: str, connection_pool) -> None:
        self.topic = topic
        self.prefix = 'mm:topic:{}'.format(self.topic)

        self.connection_pool = connection_pool
        self.SR: Optional[redis.Redis] = None

        self.num_publish = 0

    def connect(self) -> None:
        if self.SR is not None:
            return

        self.SR = redis.StrictRedis(
            connection_pool=self.connection_pool
        )

    def disconnect(self) -> None:
        if self.SR is None:
            return

        self.SR = None

    def lagger(self) -> int:
        assert self.SR is not None

        # get status of subscribers
        subscribersc: List[bytes] = self.SR.lrange(
            '{}:subscribers'.format(self.prefix),
            0, -1
        )
        decoded_subscribersc = [int(sc) for sc in subscribersc]

        # check the lagger
        minsubc = self.num_publish
        if len(decoded_subscribersc) != 0:
            minsubc = min(decoded_subscribersc)

        return minsubc

    def gc(self, lagger: int) -> None:
        assert self.SR is not None

        minhighbits = lagger >> 12

        minqname = '{}:queue:{:013X}'.format(
            self.prefix,
            minhighbits
        )

        # delete all the lists before the lagger
        queues: List[bytes] = self.SR.keys(f'{self.prefix}:queue:*')
        LOG.debug('topic {} - queues: {!r}'.format(self.topic, queues))
        queues = [q for q in queues if q.decode('utf-8') < minqname]
        LOG.debug(
            'topic {} - queues to be deleted: {!r}'.format(self.topic, queues))
        if len(queues) != 0:
            LOG.debug('topic {} - deleting {!r}'.format(
                self.topic,
                queues
            ))
            self.SR.delete(*queues)

    def publish(self, method: str, params: Optional[Dict[str, Union[str, int, bool]]] = None) -> None:
        assert self.SR is not None

        high_bits = self.num_publish >> 12
        low_bits = self.num_publish & 0xfff

        if (low_bits % 128) == 127:
            lagger = self.lagger()
            LOG.debug('topic {} - sent {} lagger {}'.format(
                self.topic,
                self.num_publish,
                lagger
            ))

            while (self.num_publish - lagger) > 1024:
                LOG.debug('topic {} - waiting lagger delta: {}'.format(
                    self.topic,
                    self.num_publish - lagger
                ))
                gevent.sleep(0.01)
                lagger = self.lagger()

            if low_bits == 0xfff:
                # we are switching to a new list, gc
                self.gc(lagger)

        msg = {
            'method': method,
            'params': params
        }

        qname = '{}:queue:{:013X}'.format(
            self.prefix,
            high_bits
        )

        self.SR.rpush(qname, json.dumps(msg))
        self.num_publish += 1


class ZMQRpcFanoutClientChannel(object):
    def __init__(self, fanout: str) -> None:
        self.socket: Optional[zmq.Socket] = None
        self.reply_socket: Optional[zmq.Socket] = None
        self.context: Optional[zmq.Context] = None

        self.fanout = fanout
        self.active_rpcs: Dict[str, dict] = {}

    def run(self) -> bool:
        if self.reply_socket is None:
            return False

        LOG.debug(
            'RPC Fanout reply recving from {}:reply'.format(self.fanout))
        try:
            body = self.reply_socket.recv_json(flags=zmq.NOBLOCK)

        except zmq.ZMQError:
            return False

        LOG.debug('RPC Fanout reply from {}:reply recvd: {!r}'.format(
            self.fanout, body))
        self.reply_socket.send_string('OK')
        LOG.debug(
            'RPC Fanout reply from {}:reply recvd: {!r} - ok'.format(self.fanout, body))

        source = body.get('source', None)
        if source is None:
            LOG.error(
                'No source in reply in ZMQRpcFanoutClientChannel {}'.format(self.fanout))
            return True

        id_ = body.get('id', None)
        if id_ is None:
            LOG.error('No id in reply in ZMQRpcFanoutClientChannel {} from {}'.format(
                self.fanout, source))
            return True
        actreq = self.active_rpcs.get(id_, None)
        if actreq is None:
            LOG.error('Unknown id {} in reply in ZMQRpcFanoutClientChannel {} from {}'.format(
                id_, self.fanout, source))
            return True

        result = body.get('result', None)
        if result is None:
            actreq['errors'] += 1
            errmsg = body.get('error', 'no error in reply')
            LOG.error(
                'Error in RPC reply from {}: {}'.format(source, errmsg))

        else:
            actreq['answers'][source] = result
        LOG.debug('RPC Fanout state: {!r}'.format(actreq))

        if len(actreq['answers'])+actreq['errors'] >= actreq['num_results']:
            actreq['event'].set({
                'answers': actreq['answers'],
                'errors': actreq['errors']
            })
            self.active_rpcs.pop(id_)

        return True

    def send_rpc(self, method: str, params: Optional[Dict[str, Union[str, int, bool]]] = None, num_results: int = 0, and_discard: bool = False) -> gevent.event.AsyncResult:
        if self.socket is None:
            raise RuntimeError('Not connected')

        if params is None:
            params = {}

        id_ = str(uuid.uuid1())

        body = {
            'reply_to': '{}:reply'.format(self.fanout),
            'method': method,
            'id': id_,
            'params': params
        }

        event = gevent.event.AsyncResult()

        if num_results == 0:
            event.set({
                'answers': {},
                'errors': 0
            })
            return event

        self.active_rpcs[id_] = {
            'cmd': method,
            'answers': {},
            'num_results': num_results,
            'event': event,
            'errors': 0,
            'discard': and_discard
        }

        LOG.debug('RPC Fanout Client: send multipart to {}: {!r}'.format(
            self.fanout, json.dumps(body)))
        self.socket.send_multipart([
            f'{self.fanout}'.encode('utf-8'),
            json.dumps(body).encode('utf-8')
        ])
        LOG.debug(
            'RPC Fanout Client: send multipart to {}: {!r} - done'.format(self.fanout, json.dumps(body)))

        gevent.sleep(0)

        return event

    def connect(self, context: zmq.Context) -> None:
        if self.socket is not None:
            return

        self.context = context

        self.socket = context.socket(zmq.PUB)
        self.socket.bind('ipc:///var/run/minemeld/{}'.format(self.fanout))

        self.reply_socket = context.socket(zmq.REP)
        self.reply_socket.bind(
            'ipc:///var/run/minemeld/{}:reply'.format(self.fanout))

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)

        if self.reply_socket is not None:
            self.reply_socket.close(linger=0)

        self.socket = None
        self.reply_socket = None


class ZMQRpcServerChannel(object):
    def __init__(self, name: str, obj: object, allowed_methods: Optional[List[str]] = None,
                 method_prefix: str = '', fanout: Optional[str] = None) -> None:
        if allowed_methods is None:
            allowed_methods = []

        self.name = name
        self.obj = obj

        self.allowed_methods = allowed_methods
        self.method_prefix = method_prefix

        self.fanout = fanout
        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None

    def _send_result(self, reply_to: bytes, id_: str, result: Optional[Any] = None, error: Optional[str] = None):
        assert self.context is not None
        assert self.socket is not None

        ans = {
            'source': self.name,
            'id': id_,
            'result': result,
            'error': error
        }

        if self.fanout is not None:
            reply_socket = self.context.socket(zmq.REQ)
            reply_socket.connect(
                'ipc:///var/run/minemeld/{}'.format(reply_to.decode('utf-8')))
            LOG.debug('RPC Server {} result to {!r}: {!r}'.format(
                self.name, reply_to, ans))
            reply_socket.send_json(ans, default=_bytes_serializer)
            reply_socket.recv()
            LOG.debug(
                'RPC Server {} result to {!r} - done'.format(self.name, reply_to))
            reply_socket.close(linger=0)
            LOG.debug(
                'RPC Server {} result to {!r} - closed'.format(self.name, reply_to))
            reply_socket = None

        else:
            self.socket.send_multipart([
                reply_to,
                b'',
                json.dumps(ans).encode('utf-8')
            ])

    def run(self) -> bool:
        if self.socket is None:
            LOG.error(
                f'Run called with invalid socket in RPC server channel: {self.name}')
            return False

        LOG.debug(f'RPC Server receiving from {self.name} - {self.fanout}')

        try:
            toks = self.socket.recv_multipart(flags=zmq.NOBLOCK)

        except zmq.ZMQError:
            return False

        LOG.debug(
            'RPC Server recvd from {} - {}: {!r}'.format(self.name, self.fanout, toks))

        if self.fanout is not None:
            reply_to, body = toks
            reply_to = reply_to+b':reply'
        else:
            reply_to, _, body = toks

        body = json.loads(body)
        LOG.debug('RPC command to {}: {!r}'.format(self.name, body))

        method = body.get('method', None)
        id_ = body.get('id', None)
        params = body.get('params', {})

        if method is None:
            LOG.error('No method in msg body')
            return True
        if id_ is None:
            LOG.error('No id in msg body')
            return True

        method = self.method_prefix+method

        if method not in self.allowed_methods:
            LOG.error(
                f'Method not allowed in RPC server channel {self.name}: {method} {self.allowed_methods}')
            self._send_result(reply_to, id_, error='Method not allowed')

        m = getattr(self.obj, method, None)
        if m is None:
            LOG.error('Method {} not defined in RPC server channel {}'.format(
                method, self.name))
            self._send_result(reply_to, id_, error='Method not defined')

        try:
            result = m(**params)

        except gevent.GreenletExit:
            raise

        except Exception as e:
            self._send_result(reply_to, id_, error=str(e))

        else:
            self._send_result(reply_to, id_, result=result)

        return True

    def connect(self, context: zmq.Context) -> None:
        if self.socket is not None:
            return

        self.context = context

        if self.fanout is not None:
            # we are subscribers
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(
                'ipc:///var/run/minemeld/{}'.format(self.fanout))
            # set the filter to empty to recv all messages
            self.socket.setsockopt(zmq.SUBSCRIBE, b'')

        else:
            # we are a router
            self.socket = self.context.socket(zmq.ROUTER)

            if self.name[0] == '@':
                address = 'ipc://@/var/run/minemeld/{}:rpc'.format(
                    self.name[1:]
                )
            else:
                address = 'ipc:///var/run/minemeld/{}:rpc'.format(
                    self.name
                )
            self.socket.bind(address)

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None


class ZMQPubChannel(object):
    def __init__(self, topic: str) -> None:
        self.socket: Optional[zmq.Socket] = None
        self.reply_socket: Optional[zmq.Socket] = None
        self.context: Optional[zmq.Context] = None
        self.topic = topic

    def publish(self, method: str, params: Optional[Dict[str, Union[int, bool, str]]] = None) -> None:
        if self.socket is None:
            raise RuntimeError('Not connected')

        if params is None:
            params = {}

        id_ = str(uuid.uuid1())

        body = {
            'method': method,
            'id': id_,
            'params': params
        }

        try:
            self.socket.send_json(
                obj=body,
                flags=zmq.NOBLOCK,
                default=_bytes_serializer
            )
        except zmq.ZMQError:
            LOG.error('Topic {} queue full - dropping message'.format(self.topic))

        gevent.sleep(0)

    def connect(self, context) -> None:
        if self.socket is not None:
            return

        self.context = context

        self.socket = context.socket(zmq.PUB)
        self.socket.bind('ipc:///var/run/minemeld/{}'.format(self.topic))

    def disconnect(self) -> None:
        if self.socket is None:
            return

        self.socket.close(linger=0)
        self.socket = None


class ZMQSubChannel(object):
    def __init__(self, name: str, obj: object, allowed_methods: Optional[List[str]] = None,
                 method_prefix: str = '', topic: Optional[str] = None):
        if allowed_methods is None:
            allowed_methods = []

        self.name = name
        self.obj = obj

        self.allowed_methods = allowed_methods
        self.method_prefix = method_prefix
        self.topic = topic

        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None

    def run(self) -> bool:
        if self.socket is None:
            LOG.error(
                'Run called with invalid socket in ZMQ Pub channel: {}'.format(self.name))
            return False

        LOG.debug('ZMQPub {} receiving'.format(self.name))
        try:
            body = self.socket.recv_json(flags=zmq.NOBLOCK)

        except zmq.ZMQError:
            return False

        LOG.debug('ZMQPub {} recvd: {!r}'.format(self.name, body))

        method = body.get('method', None)
        id_ = body.get('id', None)
        params = body.get('params', {})

        if method is None:
            LOG.error('No method in msg body')
            return True
        if id_ is None:
            LOG.error('No id in msg body')
            return True

        method = self.method_prefix+method

        if method not in self.allowed_methods:
            LOG.error(
                f'Method not allowed in RPC server channel {self.name}: {method} {self.allowed_methods}')
            return True

        m = getattr(self.obj, method, None)
        if m is None:
            LOG.error('Method {} not defined in RPC server channel {}'.format(
                method, self.name))
            return True

        try:
            m(**params)

        except gevent.GreenletExit:
            raise

        except Exception:
            LOG.exception('Exception in ZMQPub {}'.format(self.name))

        return True

    def connect(self, context: zmq.Context) -> None:
        if self.socket is not None:
            return

        self.context = context

        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect('ipc:///var/run/minemeld/{}'.format(self.topic))
        # set the filter to empty to recv all messages
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None


class RedisSubChannel(object):
    def __init__(self, topic: str, connection_pool: redis.ConnectionPool, object_: object,
                 allowed_methods: List[str], name: Optional[str] = None) -> None:
        self.topic = topic
        self.prefix = 'mm:topic:{}'.format(self.topic)
        self.channel = None
        self.name = name
        self.object = object_
        self.allowed_methods = allowed_methods
        self.connection_pool = connection_pool

        self.num_callbacks = 0

        self.sub_number: Optional[int] = None

        self.counter: int = 0
        self.subscribers_key = '{}:subscribers'.format(self.prefix)

    def _callback(self, msg: str) -> None:
        try:
            decoded_msg = json.loads(msg)
        except ValueError:
            LOG.error("invalid message received")
            return

        method = decoded_msg.get('method', None)
        params = decoded_msg.get('params', {})
        if method is None:
            LOG.error("Message without method field")
            return

        if method not in self.allowed_methods:
            LOG.error("Method not allowed: %s", method)
            return

        m = getattr(self.object, method, None)
        if m is None:
            LOG.error('Method %s not defined', method)
            return

        try:
            m(**params)

        except gevent.GreenletExit:
            raise

        except:
            LOG.exception('Exception in handling %s on topic %s '
                          'with params %s', method, self.topic, params)

        self.num_callbacks += 1

    def run(self, SR: redis.Redis) -> bool:
        base = self.counter & 0xfff
        top = min(base + 127, 0xfff)

        msgs = SR.lrange(
            '{}:queue:{:013X}'.format(self.prefix, self.counter >> 12),
            base,
            top
        )

        for m in msgs:
            LOG.debug('topic {} - {!r}'.format(
                self.topic,
                m
            ))
            self._callback(m)

        self.counter += len(msgs)

        if len(msgs) > 0:
            SR.lset(
                self.subscribers_key,
                self.sub_number,
                self.counter
            )

        return len(msgs) > 0

    def connect(self) -> None:
        subscribers_key = '{}:subscribers'.format(self.prefix)

        SR: redis.Redis = redis.StrictRedis(
            connection_pool=self.connection_pool
        )

        self.sub_number = SR.rpush(
            subscribers_key,
            0
        )
        self.sub_number -= 1
        LOG.debug('Sub Number {} on {}'.format(
            self.sub_number, subscribers_key))

    def disconnect(self) -> None:
        pass


class ZMQRedis(object):
    def __init__(self, config: dict) -> None:
        self.context = None
        self.rpc_server_channels: Dict[str, ZMQRpcServerChannel] = {}
        self.pub_channels: List[RedisPubChannel] = []
        self.mw_pub_channels: List[ZMQPubChannel] = []
        self.sub_channels: List[RedisSubChannel] = []
        self.mw_sub_channels: List[ZMQSubChannel] = []
        self.rpc_fanout_clients_channels: List[ZMQRpcFanoutClientChannel] = []

        self.active_rpcs: Dict[str, dict] = {}

        self.ioloops: List[gevent.Greenlet] = []

        self.failure_listeners: List[Callable[[], None]] = []

        self.redis_config = {
            'url': os.environ.get('REDIS_URL', 'unix:///var/run/redis/redis.sock')
        }
        self.redis_cp = redis.ConnectionPool.from_url(
            self.redis_config['url']
        )

    def add_failure_listener(self, listener: Callable[[], None]) -> None:
        self.failure_listeners.append(listener)

    def request_rpc_server_channel(self, name: str, obj: Optional[object] = None, allowed_methods: Optional[List[str]] = None,
                                   method_prefix: str = '', fanout: Optional[str] = None) -> None:
        if allowed_methods is None:
            allowed_methods = []

        if name in self.rpc_server_channels:
            return

        self.rpc_server_channels[name] = ZMQRpcServerChannel(
            name,
            obj,
            method_prefix=method_prefix,
            allowed_methods=allowed_methods,
            fanout=fanout
        )

    def request_rpc_fanout_client_channel(self, topic: str) -> ZMQRpcFanoutClientChannel:
        c = ZMQRpcFanoutClientChannel(topic)
        self.rpc_fanout_clients_channels.append(c)
        return c

    def request_pub_channel(self, topic: str, multi_write: bool = False) -> Union[ZMQPubChannel, RedisPubChannel]:
        if not multi_write:
            redis_pub_channel = RedisPubChannel(
                topic=topic,
                connection_pool=self.redis_cp
            )
            self.pub_channels.append(redis_pub_channel)

            return redis_pub_channel

        zmq_pub_channel = ZMQPubChannel(topic=topic)
        self.mw_pub_channels.append(zmq_pub_channel)

        return zmq_pub_channel

    def request_sub_channel(self, topic, obj=None, allowed_methods=None,
                            name=None, max_length=None, multi_write=False) -> None:
        if allowed_methods is None:
            allowed_methods = []

        if not multi_write:
            redis_subchannel = RedisSubChannel(
                topic=topic,
                connection_pool=self.redis_cp,
                object_=obj,
                allowed_methods=allowed_methods,
                name=name
            )
            self.sub_channels.append(redis_subchannel)

            return

        zmq_subchannel = ZMQSubChannel(
            name=name,
            obj=obj,
            allowed_methods=allowed_methods,
            topic=topic
        )
        self.mw_sub_channels.append(zmq_subchannel)

    def send_rpc(self, dest: str, method: str, params: Optional[Dict[str, Union[str, int, bool]]],
                 block: bool = True, timeout: Optional[int] = None) -> Optional[Any]:
        if self.context is None:
            LOG.error('send_rpc to {} when not connected'.format(dest))
            return None

        id_ = str(uuid.uuid1())

        body = {
            'method': method,
            'id': id_,
            'params': params
        }

        socket = self.context.socket(zmq.REQ)

        if dest[0] == '@':
            address = 'ipc://@/var/run/minemeld/{}:rpc'.format(
                dest[1:]
            )
        else:
            address = 'ipc:///var/run/minemeld/{}:rpc'.format(
                dest
            )

        socket.connect(address)
        socket.setsockopt(zmq.LINGER, 0)
        socket.send_json(body, default=_bytes_serializer)
        LOG.debug('RPC sent to {}:rpc for method {}'.format(dest, method))

        if not block:
            socket.close(linger=0)
            return

        if timeout is not None:
            # zmq green does not support RCVTIMEO
            if socket.poll(flags=zmq.POLLIN, timeout=int(timeout*1000)) != 0:
                result = socket.recv_json(flags=zmq.NOBLOCK)

            else:
                socket.close(linger=0)
                raise RuntimeError('Timeout in RPC')

        else:
            result = socket.recv_json()

        socket.close(linger=0)

        return result

    def _ioloop(self) -> None:
        SR = redis.StrictRedis(connection_pool=self.redis_cp)

        executors: List[Union[ZMQRpcFanoutClientChannel,RedisSubChannel,ZMQSubChannel]] = []
        for rfcc in self.rpc_fanout_clients_channels:
            executors.append(rfcc)

        for mwschannel in self.mw_sub_channels:
            executors.append(mwschannel)

        for schannel in self.sub_channels:
            executors.append(schannel)

        while True:
            msg_handled = False

            for rpcc in self.rpc_server_channels.values():
                while rpcc.run():
                    msg_handled = True

            now = time.time()
            while (time.time() - now) < 0.1:
                if len(executors) == 0:
                    break

                executor = executors.pop(0)
                executors.append(executor)

                if isinstance(executor, RedisSubChannel):
                    result = executor.run(SR)
                else:
                    result = executor.run()

                msg_handled = msg_handled or result

            gevent.sleep(0 if msg_handled else 0.1)

    def _ioloop_failure(self, g: gevent.Greenlet) -> None:
        LOG.error('_ioloop_failure')

        try:
            g.get()

        except gevent.GreenletExit:
            return

        except:
            LOG.exception("_ioloop_failure: exception in ioloop")
            for l in self.failure_listeners:
                l()

    def start(self, start_dispatching: bool = True) -> None:
        self.context = zmq.Context()

        for rfcc in self.rpc_fanout_clients_channels:
            rfcc.connect(self.context)

        for rpcc in self.rpc_server_channels.values():
            rpcc.connect(self.context)

        for sc in self.sub_channels:
            sc.connect()

        for mwsc in self.mw_sub_channels:
            mwsc.connect(self.context)

        for pc in self.pub_channels:
            pc.connect()

        for mwpc in self.mw_pub_channels:
            mwpc.connect(self.context)

        if start_dispatching:
            self.start_dispatching()

    def start_dispatching(self) -> None:
        g = gevent.spawn(self._ioloop)
        self.ioloops.append(g)
        g.link_exception(self._ioloop_failure)

    def stop(self) -> None:
        # kill ioloops
        for j in range(len(self.ioloops)):
            self.ioloops[j].unlink(self._ioloop_failure)
            self.ioloops[j].kill()
            self.ioloops[j] = None
        self.ioloops = []

        # close channels
        for rpcc in self.rpc_server_channels.values():
            try:
                rpcc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        for pc in self.pub_channels:
            try:
                pc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        for mwpc in self.mw_pub_channels:
            try:
                mwpc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        for sc in self.sub_channels:
            try:
                sc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        for mwsc in self.mw_sub_channels:
            try:
                mwsc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        for rfc in self.rpc_fanout_clients_channels:
            try:
                rfc.disconnect()
            except Exception:
                LOG.debug("exception in disconnect: ", exc_info=True)

        if self.context is not None:
            self.context.destroy()

    @staticmethod
    def cleanup(config: dict) -> None:
        redis_cp: Optional[redis.ConnectionPool] = redis.ConnectionPool.from_url(
            os.environ.get('REDIS_URL', 'unix:///var/run/redis/redis.sock')
        )
        assert redis_cp is not None

        SR: Optional[redis.Redis] = redis.StrictRedis(connection_pool=redis_cp)
        assert SR is not None

        tkeys: List[bytes] = SR.keys(pattern='mm:topic:*')
        if len(tkeys) > 0:
            LOG.info('Deleting old keys: {}'.format(len(tkeys)))
            SR.delete(*tkeys)

        SR = None
        redis_cp = None
