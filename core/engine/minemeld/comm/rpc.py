import json
import logging
import os.path
import uuid
from typing import (
    Optional, Dict, Union, List, Any
)

import gevent
import zmq.green as zmq


LOG = logging.getLogger(__name__)


def _bytes_serializer(o: Any) -> str:
    raise TypeError(f"ZMQRedis: {o} not JSON serializable")


class Client:
    def __init__(self, path: str, fanout: str) -> None:
        self.socket: Optional[zmq.Socket] = None
        self.reply_socket: Optional[zmq.Socket] = None
        self.context: Optional[zmq.Context] = None

        self.path = path
        self.fanout = fanout
        self.active_rpcs: Dict[str, dict] = {}

    def run(self) -> bool:
        if self.reply_socket is None:
            return False

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
        self.socket.bind(f'ipc://{os.path.join(self.path, self.fanout)}')

        self.reply_socket = context.socket(zmq.REP)
        self.reply_socket.bind(f'ipc://{os.path.join(self.path, self.fanout)}:reply')

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)

        if self.reply_socket is not None:
            self.reply_socket.close(linger=0)

        self.socket = None
        self.reply_socket = None


class Server:
    def __init__(self, name: str, path: str, obj: object, allowed_methods: Optional[List[str]] = None,
                 method_prefix: str = '', fanout: Optional[str] = None) -> None:
        if allowed_methods is None:
            allowed_methods = []

        self.name = name
        self.obj = obj
        self.path = path

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
                f'ipc://{os.path.join(self.path, reply_to.decode("utf-8"))}'
            )
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
                f'ipc://{os.path.join(self.path, self.fanout)}')
            # set the filter to empty to recv all messages
            self.socket.setsockopt(zmq.SUBSCRIBE, b'')

        else:
            # we are a router
            self.socket = self.context.socket(zmq.ROUTER)

            if self.name[0] == '@':
                address = f'ipc://@{os.path.join(self.path, self.name[1:])}:rpc'
            else:
                address = f'ipc://{os.path.join(self.path, self.name)}:rpc'
            self.socket.bind(address)

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None


class Reactor:
    def __init__(self, path: str, context: zmq.Context):
        self.path = path
        self.clients: List[Client] = []
        self.servers: List[Server] = []
        self.context = context

    def new_client(self, fanout: str) -> Client:
        c = Client(
            path=self.path,
            fanout=fanout
        )
        self.clients.append(c)
        return c

    def new_server(self, name: str, obj: object, allowed_methods: Optional[List[str]] = None,
                 method_prefix: str = '', fanout: Optional[str] = None) -> Server:
        s = Server(
            name=name,
            path=self.path,
            obj=obj,
            allowed_methods=allowed_methods,
            method_prefix=method_prefix,
            fanout=fanout
        )
        self.servers.append(s)
        return s

    def select(self, timeout: Optional[float] = None) -> List[Any]:
        rlist = [c.reply_socket for c in self.clients if c.reply_socket is not None]
        rlist.extend(
            [s.socket for s in self.servers if s.socket is not None]
        )

        poll = zmq.Poller()
        for s in rlist:
            poll.register(s)
        ready = [s[0] for s in poll.poll(timeout=None)]
        # ready = zmq.select(
        #     rlist=rlist,
        #     wlist=[],
        #     xlist=[],
        #     timeout=timeout
        # )

        result = [c for c in self.clients if c.reply_socket in ready]
        result.extend([s for s in self.servers if s.socket in ready])

        return result

    def run(self):
        # for testing
        while True:
            ready = self.select()
            for r in ready:
                r.run()

    def connect(self):
        for c in self.clients:
            c.connect(self.context)

        for s in self.servers:
            s.connect(self.context)

    def disconnect(self):
        for c in self.clients:
            try:
                c.disconnect()
            except Exception:
                LOG.error(f'Exception in disconnecting RPC Client: ', exc_info=True)

        for s in self.servers:
            try:
                s.disconnect()
            except Exception:
                LOG.error(f'Exception in disconnecting RPC Client: ', exc_info=True)
