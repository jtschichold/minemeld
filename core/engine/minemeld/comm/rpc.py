import json
import logging
import os.path
import uuid
from typing import (
    Optional, Dict, Union, List, Any, Protocol,
    TypedDict, Callable, Mapping
)

import gevent
import zmq.green as zmq
from zmq.sugar import NOBLOCK, REQ, REP, ROUTER, DEALER, IDENTITY


LOG = logging.getLogger(__name__)
# LOG.setLevel(level=logging.DEBUG)


class Dispatcher(Protocol):
    def run(self) -> bool:
        ...


class RouterCallback(Protocol):
    def __call__(self, method: str, **kwargs) -> gevent.event.AsyncResult:
        ...


class ServerCallback(Protocol):
    def __call__(self, node: str, method: str, **kwargs) -> gevent.event.AsyncResult:
        ...


RPCRequestParams = Mapping[str, Union[str, int, bool, dict]]


class RPCRequest(TypedDict):
    _id: str
    method: str
    params: RPCRequestParams
    reply_to: Optional[str]
    node: Optional[str]


class RPCResponse(TypedDict):
    _id: str
    result: Optional[Any]
    error: Optional[Any]


class RPCAsyncResult(TypedDict):
    _id: str
    async_result: gevent.event.AsyncResult
    reply_to: str


def _bytes_serializer(o: Any) -> str:
    raise TypeError(f"ZMQRedis: {o} not JSON serializable")


def _endpoint_to_address(path: str, endpoint: str) -> str:
    if endpoint[0] == '@':
        return f'ipc://@{os.path.join(path, endpoint[1:])}'

    return f'ipc://{os.path.join(path, endpoint)}'


class AsyncClient:
    def __init__(self, path: str, name: str) -> None:
        self.reply_socket: Optional[zmq.Socket] = None
        self.context: Optional[zmq.Context] = None

        self.path = path
        self.name = name
        self.active_rpcs: Dict[str, dict] = {}
        self.active_sockets: Dict[str, zmq.Socket] = {}

    def run(self) -> bool:
        if self.reply_socket is None:
            return False

        try:
            toks = self.reply_socket.recv_multipart(flags=NOBLOCK)
            LOG.debug(f'RPC Client {self.name} - recvd reply: {toks}')

        except zmq.ZMQError:
            return False

        body: RPCResponse = json.loads(toks[1])

        id_ = body.get('_id', None)
        if id_ is None:
            LOG.error(f'No id in RPC reply to {self.name}')
            return True
        actreq = self.active_rpcs.get(id_, None)
        if actreq is None:
            LOG.error(f'Unknown id {id_} in reply to {self.name}')
            return True

        result = body.get('result', None)
        if result is None:
            errmsg = body.get('error', 'no error in reply')
            actreq['error'] = errmsg
            LOG.error(f'Error in RPC reply to {self.name}: {errmsg}')

        else:
            actreq['answer'] = result['answer']

        actreq['event'].set({
            'answer': actreq['answer'],
            'error': actreq.get('error', None)
        })
        self.active_rpcs.pop(id_)

        return True

    def send_rpc(self, remote: str, method: str, params: Optional[RPCRequestParams] = None, node: Optional[str] = None, ignore_answer: Optional[bool] = False) -> Optional[gevent.event.AsyncResult]:
        assert self.context is not None

        if params is None:
            params = {}

        _id = str(uuid.uuid1())

        body: RPCRequest = {
            'reply_to': f'{self.name}:reply' if not ignore_answer else None,
            'method': method,
            '_id': _id,
            'params': params,
            'node': node
        }

        result = None
        if not ignore_answer:
            result = gevent.event.AsyncResult()

            self.active_rpcs[_id] = {
                'cmd': method,
                'answer': None,
                'event': result
            }

        socket = self.active_sockets.get(remote, None)
        if socket is None:
            socket = self.context.socket(DEALER)
            socket.setsockopt(IDENTITY, (self.name+':'+remote).encode('utf-8'))
            socket.connect(_endpoint_to_address(self.path, remote))
            self.active_sockets[remote] = socket
        assert socket is not None

        LOG.debug(f'RPC Client {self.name} - Sending {method}:{_id} to {_endpoint_to_address(self.path, remote)}')

        socket.send_json(body)
        LOG.debug(f'RPC Client {self.name} - Sending {method} to {_endpoint_to_address(self.path, remote)} - done')

        gevent.sleep(0)

        return result

    def connect(self, context: zmq.Context) -> None:
        if self.reply_socket is not None:
            return

        self.context = context

        self.reply_socket = context.socket(ROUTER)
        assert self.reply_socket is not None

        endpoint_address = _endpoint_to_address(self.path, f'{self.name}:reply')
        LOG.debug(f'RPC Client for {self.name} on {endpoint_address}')
        self.reply_socket.bind(endpoint_address)

    def disconnect(self) -> None:
        for s in self.active_sockets.values():
            s.setsockopt(zmq.LINGER, 0)
            s.close(linger=0)
        self.active_sockets = {}

        if self.reply_socket is not None:
            self.reply_socket.close(linger=0)

        self.socket = None
        self.reply_socket = None


class Server:
    def __init__(self, name: str, path: str, handler: ServerCallback, new_async_result: gevent.event.Event, timeout: Optional[float] = 10.0) -> None:
        self.name = name
        self.path = path
        self.handler = handler
        self.timeout = timeout

        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None
        self.async_results: List[RPCAsyncResult] = []
        self.active_client_sockets: Dict[str, zmq.Socket] = {}
        self.new_async_result: gevent.event.Event = new_async_result

    def _send_sync_result(self, reply_to: bytes, _id: str, result: Optional[Any] = None, error: Optional[str] = None):
        assert self.socket is not None

        self.socket.send_multipart([
            reply_to,
            b'',
            json.dumps(self._build_response(_id, result, error)).encode('utf-8')
        ])

    def _send_async_result(self, reply_to: str, _id: str, result: Optional[Any] = None, error: Optional[str] = None):
        assert self.context is not None

        LOG.debug(f'RPC Server {self.name} - sending reply {_id} to {reply_to}')

        remote = _endpoint_to_address(self.path, reply_to)
        reply_socket = self.active_client_sockets.get(remote, None)
        if reply_socket is None:
            reply_socket = self.context.socket(DEALER)
            reply_socket.connect(remote)
            reply_socket.setsockopt(IDENTITY, self.name.encode('utf-8'))
            self.active_client_sockets[remote] = reply_socket

        reply_socket.send_json(self._build_response(_id, result, error))
        LOG.debug(f'RPC Server {self.name} - sending reply {_id} to {reply_to} - sent')

    def _build_response(self, _id: str, result: Optional[Any] = None, error: Optional[str] = None) -> RPCResponse:
        return {
            "_id": _id,
            "error": error,
            "result": result
        }

    def run_async_results(self) -> None:
        ready_results = [ar for ar in self.async_results if ar['async_result'].ready()]
        LOG.debug(f'RPC Server {self.name} - async results ready: {len(ready_results)}')
        if len(ready_results) == 0:
            LOG.info(f'RPC Server - 0 results')
            return

        self.async_results = [ar for ar in self.async_results if not ar['async_result'].ready()]

        for ar in ready_results:
            try:
                ans = ar['async_result'].get(block=False)

            except gevent.greenlet.GreenletExit:
                raise

            except gevent.Timeout:
                raise

            except Exception as e:
                self._send_async_result(
                    reply_to=ar['reply_to'],
                    _id=ar['_id'],
                    error=str(e)
                )

            else:
                self._send_async_result(
                    reply_to=ar['reply_to'],
                    _id=ar['_id'],
                    result=ans
                )

    def run(self) -> bool:
        if self.socket is None:
            LOG.error(
                f'Run called with invalid socket in RPC server channel: {self.name}')
            return False

        try:
            toks = self.socket.recv_multipart(flags=NOBLOCK)
            LOG.debug(f'RPC Server {self.name} - recevd message {toks}')

        except zmq.ZMQError:
            return False

        req_reply_to, body = toks

        request: RPCRequest = json.loads(body)
        LOG.debug(f'RPC Server {self.name} - recvd msg: {body}')

        method = request.get('method', None)
        _id = request.get('_id', None)
        params = request.get('params', {})
        node = request.get('node', None)
        reply_to = request.get('reply_to', None)

        if method is None:
            LOG.error('No method in msg body')
            return True
        if _id is None:
            LOG.error('No id in msg body')
            return True
        if node is None:
            LOG.error('No node in msg body')
            return True

        self.handle_request(node, method, params, _id, req_reply_to, reply_to)
        return True

    def handle_request(self, node: str, method: str, params: RPCRequestParams, _id: str, req_reply_to: bytes, reply_to: Optional[str]):
        try:
            LOG.debug(f'RPC Server {self.name} - handling message: {node}/{method}/{params}')
            result = self.handler(node=node, method=method, **params)

        except gevent.GreenletExit:
            raise

        except Exception as e:
            LOG.error(f'RPC Server {self.name} - Error handling {method} for {node}', exc_info=True)
            if reply_to == '<sync>':
                self._send_sync_result(
                    req_reply_to, _id, error=str(e)
                )
            return

        LOG.debug(f'RPC Server {self.name} - handling response {method} {reply_to}')

        if reply_to is None:
            return

        if reply_to != '<sync>':
            self.async_results.append({
                '_id': _id,
                'reply_to': reply_to,
                'async_result': result
            })
            self.new_async_result.set()
            return

        try:
            ans = result.get(timeout=self.timeout)

        except gevent.GreenletExit:
            raise

        except Exception as e:
            self._send_sync_result(
                req_reply_to, _id, error=str(e)
            )

        else:
            self._send_sync_result(
                req_reply_to, _id, result=ans
            )

        return True

    def connect(self, context: zmq.Context) -> None:
        if self.socket is not None:
            return

        self.context = context

        self.socket = self.context.socket(ROUTER)
        LOG.debug(f'RPC server for {self.name} on {_endpoint_to_address(self.path, self.name)}')
        self.socket.bind(_endpoint_to_address(self.path, self.name))

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

        for s in self.active_client_sockets.values():
            s.setsockopt(zmq.LINGER, 0)
            s.close(linger=0)


class Router:
    def __init__(self, name: str, path: str, routing_table: Dict[str, str], handler: RouterCallback, new_async_result: gevent.event.Event, timeout: Optional[float] = 10.0) -> None:
        self.name = name
        self.path = path
        self.routing_table = routing_table
        self.handler = handler
        self.timeout = timeout

        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None
        self.remotes: Dict[str,zmq.Socket] = {}
        self.async_results: List[RPCAsyncResult] = []
        self.active_client_sockets: Dict[str, zmq.Socket] = {}
        self.new_async_result: gevent.event.Event = new_async_result

    def _send_sync_result(self, reply_to: bytes, _id: str, result: Optional[Any] = None, error: Optional[str] = None):
        assert self.socket is not None

        self.socket.send_multipart([
            reply_to,
            b'',
            json.dumps(self._build_response(_id, result, error)).encode('utf-8')
        ])

    def _send_async_result(self, reply_to: str, _id: str, result: Optional[Any] = None, error: Optional[str] = None):
        assert self.context is not None

        remote = _endpoint_to_address(self.path, reply_to)
        reply_socket = self.active_client_sockets.get(remote, None)
        if reply_socket is None:
            reply_socket = self.context.socket(DEALER)
            reply_socket.connect(remote)
            reply_socket.setsockopt(IDENTITY, self.name.encode('utf-8'))
            self.active_client_sockets[remote] = reply_socket

        reply_socket.send_json(self._build_response(_id, result, error))

    def _build_response(self, _id: str, result: Optional[Any] = None, error: Optional[str] = None) -> RPCResponse:
        return {
            "_id": _id,
            "error": error,
            "result": result
        }

    def run_async_results(self) -> None:
        ready_results = [ar for ar in self.async_results if ar['async_result'].ready()]
        if len(ready_results) == 0:
            return

        self.async_results = [ar for ar in self.async_results if not ar['async_result'].ready()]

        for ar in ready_results:
            try:
                ans = ar['async_result'].get(block=False)

            except gevent.greenlet.GreenletExit:
                raise

            except gevent.Timeout:
                raise

            except Exception as e:
                self._send_async_result(
                    reply_to=ar['reply_to'],
                    _id=ar['_id'],
                    error=str(e)
                )

            else:
                self._send_async_result(
                    reply_to=ar['reply_to'],
                    _id=ar['_id'],
                    result=ans
                )

    def run(self) -> bool:
        if self.socket is None:
            LOG.error(
                f'Run called with invalid socket in RPC server channel: {self.name}')
            return False

        try:
            toks = self.socket.recv_multipart(flags=NOBLOCK)
            LOG.debug(f'RPC Router {self.name} - recved message {toks}')

        except zmq.ZMQError:
            return False

        req_reply_to, body = toks

        LOG.debug(f'RPC Router {self.name} - recvd msg: {body}')
        request: RPCRequest = json.loads(body)

        method = request.get('method', None)
        _id = request.get('_id', None)
        params = request.get('params', {})
        node = request.get('node', None)
        reply_to = request.get('reply_to', None)

        if method is None:
            LOG.error('No method in msg body')
            return True
        if _id is None:
            LOG.error('No id in msg body')
            return True

        if node is None:
            # local request
            self.handle_local_request(method, params, _id, req_reply_to, reply_to)
            return True

        self.handle_remote_request(node, _id, request, req_reply_to)
        return True

    def handle_local_request(self, method: str, params: RPCRequestParams, _id: str, req_reply_to: bytes, reply_to: Optional[str]):
        try:
            result = self.handler(method, **params)

        except gevent.GreenletExit:
            raise

        except Exception as e:
            self._send_sync_result(
                req_reply_to, _id, error=str(e)
            )
            return

        if reply_to is None:
            return        

        if reply_to != '<sync>':
            self.async_results.append({
                '_id': _id,
                'reply_to': reply_to,
                'async_result': result
            })
            self.new_async_result.set()
            return

        try:
            ans = result.get()

        except gevent.GreenletExit:
            raise

        except Exception as e:
            self._send_sync_result(
                req_reply_to, _id, error=str(e)
            )

        else:
            self._send_sync_result(
                req_reply_to, _id, result=ans
            )

    def handle_remote_request(self, node: str, _id: str, request: RPCRequest, req_reply_to: bytes):
        if (remote_endpoint := self.routing_table.get(node, None)) is None:
            self._send_sync_result(req_reply_to, _id, error=f'Unknown node {node}')
            return

        if (req_socket := self.remotes.get(remote_endpoint, None)) is None:
            self._send_sync_result(req_reply_to, _id, error=f'Internal error - socket to remote endpoint for {node} is not connected')
            return

        req_socket.send_json(request)
        ans: RPCResponse = req_socket.recv_json()

        self._send_sync_result(req_reply_to, ans['_id'], error=ans['error'], result=ans['result'])

    def connect(self, context: zmq.Context) -> None:
        if self.socket is not None:
            return

        self.context = context

        self.socket = self.context.socket(ROUTER)
        LOG.debug(f'RPC router for {self.name} on {_endpoint_to_address(self.path, self.name)}')
        self.socket.bind(_endpoint_to_address(self.path, self.name))

        for remote_endpoint in self.routing_table.values():
            if remote_endpoint in self.remotes:
                continue

            self.remotes[remote_endpoint] = self.context.socket(REQ)
            self.remotes[remote_endpoint].connect(_endpoint_to_address(self.path, remote_endpoint))

    def disconnect(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

        for s in self.remotes.values():
            s.close(linger = 0)
        self.remotes = {}

        for s in self.active_client_sockets.values():
            s.setsockopt(zmq.LINGER, 0)
            s.close(linger=0)


def request(context: zmq.Context, path: str, dest: str, method: str, params: Optional[Dict[str, Union[str, int, bool]]],
            block: bool = True, timeout: Optional[int] = None) -> Optional[RPCResponse]:
    _id = str(uuid.uuid1())
    body: RPCRequest = {
        'reply_to': '<sync>',
        'node': None,
        'method': method,
        '_id': _id,
        'params': params or {}
    }

    socket = context.socket(REQ)
    socket.connect(_endpoint_to_address(path, dest))
    socket.setsockopt(zmq.LINGER, 0)
    socket.send_json(body, default=_bytes_serializer)

    if not block:
        socket.close(linger=0)
        return None

    if timeout is not None:
        # zmq green does not support RCVTIMEO
        if socket.poll(flags=zmq.POLLIN, timeout=int(timeout * 1000)) != 0:
            result = socket.recv_json(flags=NOBLOCK)

        else:
            socket.close(linger=0)
            raise RuntimeError('Timeout in RPC')

    else:
        result = socket.recv_json()

    socket.close(linger=0)

    return result


class Reactor:
    def __init__(self, path: str, context: zmq.Context):
        self.path = path
        self.clients: List[AsyncClient] = []
        self.servers: List[Server] = []
        self.routers: List[Router] = []
        self.context = context
        self.new_async_result: gevent.event.Event = gevent.event.Event()

    def new_client(self, name: str) -> AsyncClient:
        c = AsyncClient(
            path=self.path,
            name=name
        )
        self.clients.append(c)
        return c

    def new_server(self, name: str, handler: Callable) -> Server:
        s = Server(
            name=name,
            path=self.path,
            handler=handler,
            new_async_result=self.new_async_result
        )
        self.servers.append(s)
        return s

    def new_router(self, name: str, handler: RouterCallback, routing_table: Dict[str, str] = {}) -> Router:
        r = Router(
            name=name,
            path=self.path,
            handler=handler,
            routing_table=routing_table,
            new_async_result=self.new_async_result
        )
        self.routers.append(r)
        return r

    def select(self, timeout: Optional[int] = None) -> List[Dispatcher]:
        rlist = [c.reply_socket for c in self.clients if c.reply_socket is not None]
        rlist.extend(
            [s.socket for s in self.servers if s.socket is not None]
        )
        rlist.extend(
            [r.socket for r in self.routers if r.socket is not None]
        )

        LOG.debug(f'RPC Reactor - poll {len(rlist)} sockets')
        poll = zmq.Poller()
        for r in rlist:
            poll.register(r, zmq.POLLIN)
        ready = [sckt[0] for sckt in poll.poll(timeout=timeout)]

        LOG.debug(f'RPC Reactor - poll {len(ready)} ready')

        result: List[Dispatcher] = [c for c in self.clients if c.reply_socket in ready]
        result.extend([s for s in self.servers if s.socket in ready])
        result.extend([r for r in self.routers if r.socket in ready])

        return result

    def get_async_results(self) -> List[gevent.event.AsyncResult]:
        result: List[gevent.event.AsyncResult] = []

        for s in self.servers:
            result.extend([ar['async_result'] for ar in s.async_results])

        for r in self.routers:
            result.extend([ar['async_result'] for ar in r.async_results])

        return result

    def dispatch_async_results(self):
        LOG.debug(f'RPC Reactor - dispatch server async results')
        for s in self.servers:
            s.run_async_results()

        LOG.debug(f'RPC Reactor - dispatch routers async results')
        for r in self.routers:
            r.run_async_results()

    def connect(self):
        for c in self.clients:
            c.connect(self.context)

        for s in self.servers:
            s.connect(self.context)

        for r in self.routers:
            r.connect(self.context)

    def disconnect(self):
        for c in self.clients:
            try:
                c.disconnect()
            except Exception:
                LOG.error(f'Exception in disconnecting RPC Client: ', exc_info=True)
        self.clients = []

        for s in self.servers:
            try:
                s.disconnect()
            except Exception:
                LOG.error(f'Exception in disconnecting RPC Server: ', exc_info=True)
        self.servers = []

        for r in self.routers:
            try:
                r.disconnect()
            except Exception:
                LOG.error(f'Exception in disconnecting RPC Router: ', exc_info=True)
        self.routers = []

