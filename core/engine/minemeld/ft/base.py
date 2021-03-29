from typing import (
    Optional, Dict, Any, List, TypedDict
)
from collections import deque
import logging

import gevent
import gevent.queue
import gevent.event

from minemeld.chassis import Chassis
from minemeld.comm.pubsub import Publisher
from minemeld.config import TMineMeldNodeConfig

from . import ft_states, ChassisNode
from .utils import utc_millisec, GThrottled


HIGH_PRIORITY_METHODS = [
    'checkpoint',
    'init',
    'configure',
    'signal'
]

LOG = logging.getLogger(__name__)


class Message(TypedDict):
    async_answer: Optional[gevent.event.AsyncResult]
    method: str
    args: dict


class MultiQueue:
    def __init__(self, maxsize: List[Optional[int]]):
        self.queues: List[gevent.queue.Queue] = [gevent.queue.Queue(maxsize=n) for n in maxsize]
        self.events: List[gevent.event.Event] = [gevent.event.Event() for _ in maxsize]
        self.start_wait: List[gevent.event.Event] = [gevent.event.Event() for _ in maxsize]
        self.glets: List[gevent.Greenlet] = []
        self.waiting: Optional[gevent.Greenlet] = None

    def put(self, p: int, item: Message, block: bool = True, timeout: Optional[float] = None) -> None:
        assert p < len(self.queues)

        self.queues[p].put(item, block=block, timeout=timeout)

    def get(self, p: int, block: bool = True, timeout: Optional[float] = None) -> Message:
        assert p < len(self.queues)

        return self.queues[p].get(block=block, timeout=timeout)

    def peek(self, p: int, block: bool = True, timeout: Optional[float] = None) -> Message:
        assert p < len(self.queues)

        return self.queues[p].peek(block=block, timeout=timeout)

    def __iter__(self):
        return self

    def __next__(self) -> Message:
        assert self.waiting is None

        self.waiting = gevent.getcurrent()

        try:
            for i in range(len(self.queues)):
                self.events[i].clear()
                self.start_wait[i].set()

            LOG.debug(f'MQ: Waiting')
            gevent.wait(objects=self.events, timeout=None, count=1)
            LOG.debug(f'MQ: Waiting done')
            for i in range(len(self.queues)):
                if self.events[i].is_set():
                    LOG.debug(f'MQ: Returning from {i}')
                    return self.queues[i].get()

        finally:
            self.waiting = None

        raise StopIteration

    def _check_loop(self, i: int):
        while True:
            self.start_wait[i].wait()
            self.start_wait[i].clear()
            LOG.debug(f'MQ:{i} wait')
            self.queues[i].peek()
            self.events[i].set()
            LOG.debug(f'MQ:{i} set')

    def start(self):
        self.glets = [gevent.spawn(self._check_loop, i) for i in range(len(self.queues))]

    def stop(self):
        if self.glets is not None:
            for g in self.glets:
                g.kill()


class BaseFT:
    def __init__(self, name: str, chassis: Chassis, num_inputs: int):
        self.name = name
        self.chassis = chassis
        self.num_inputs = num_inputs

        self.state = ft_states.READY
        self.last_checkpoint: Optional[str] = None

        self.msg_queue: MultiQueue = MultiQueue(maxsize=[1, 2048])
        self.dispatcher_glet: Optional[gevent.Greenlet] = None

        self._last_status_publish: Optional[float] = None
        self._throttled_publish_status = GThrottled(self._internal_publish_status, 3000)
        self._clock = 0

        self.statistics: Dict[str, int] = {}

        self.publisher: Optional[Publisher] = None

    def set_state(self, new_state: int) -> None:
        self.state = new_state
        self.publish_status(force=True)

    def configure(self, config: TMineMeldNodeConfig) -> None:
        pass # XXX - TBD

    def connect(self, p: Publisher) -> None:
        self.publisher = p

    # status related methods
    def publish_status(self, force=False):
        if force:
            self._internal_publish_status()

        self._throttled_publish_status()

    def _internal_publish_status(self):
        self._last_status_publish = utc_millisec()
        status = self.get_status()
        self.chassis.publish_status(
            timestamp=self._last_status_publish,
            nodename=self.name,
            status=status
        )

    def get_status(self) -> Dict[str, Any]:
        result = {
            'clock': self._clock,
            'state': self.state,
            'statistics': self.statistics,
        }
        self._clock += 1
        return result

    # publish method
    def publish_checkpoint(self, value: str) -> None:
        if self.publisher is None:
            return

        self.publisher.publish(
            method='checkpoint',
            params={
                'source': self.name,
                'value': value
            }
        )

    # checkpoint methods
    def create_checkpoint(self, value: str) -> None:
        pass

    def remove_checkpoint(self) -> None:
        pass

    # state switch methods, should be implemented
    # in subclasses
    def initialize(self) -> None:
        pass

    def rebuild(self) -> None:
        pass

    def reset(self) -> None:
        pass

    # rpc request handler methods
    def on_state_info(self):
        return {
            'checkpoint': self.last_checkpoint,
            'state': self.state,
            'is_source': self.num_inputs == 0
        }

    def on_init(self, next_state: str) -> str:
        assert next_state in ['initialize', 'rebuild', 'reset']

        if next_state == 'rebuild':
            self.msg_queue.put(1, item={
                'async_answer': None,
                'method': 'rebuild',
                'args': {}
            })
        elif next_state == 'reset':
            self.msg_queue.put(1, item={
                'async_answer': None,
                'method': 'reset',
                'args': {}
            })

        elif next_state == 'initialize':
            self.initialize()

        self.set_state(ft_states.INIT)
        self.remove_checkpoint()

        return 'OK'

    def on_checkpoint(self, value: str) -> str:
        if self.num_inputs != 0:
            return 'ignored'

        self.set_state(ft_states.IDLE)
        self.create_checkpoint(value)
        self.last_checkpoint = value
        self.publish_checkpoint(value)

        return 'OK'

    def _dispatcher(self):
        msg: Message
        for msg in self.msg_queue:
            LOG.debug(f'{self.name} - dispatch {msg}')
            if msg['method'] == 'checkpoint':
                value: Optional[str] = msg.get('args', {}).get('value', None)
                assert value is not None

                result = self.on_checkpoint(value)

                if msg['async_answer'] is not None:
                    msg['async_answer'].set(value=result)
                continue

            if msg['method'] == 'rebuild':
                assert msg['async_answer'] is None

                self.rebuild()
                continue

            if msg['method'] == 'reset':
                assert msg['async_answer'] is None

                self.reset()
                continue

            # XXX - handle fabric messages

    def on_msg(self, method: str, **kwargs) -> Optional[gevent.event.AsyncResult]:
        LOG.debug(f'{self.name} - recv {method} args: {kwargs}')
        async_answer = gevent.event.AsyncResult()
        if method == 'state_info':
            async_answer.set(value=self.on_state_info())
            return async_answer

        if method == 'status':
            async_answer.set(value=self.get_status())
            return async_answer

        if method == 'init':
            command: Optional[str] = kwargs.get('command', None)
            assert command is not None

            async_answer.set(value=self.on_init(command))
            return async_answer

        self.msg_queue.put(
            0 if method in HIGH_PRIORITY_METHODS else 1,
            item={
                'async_answer': async_answer,
                'method': method,
                'args': kwargs
            }
        )
        LOG.debug(f'{self.name} - queued')

        return async_answer

    def start_dispatch(self) -> None:
        assert self.dispatcher_glet is None

        LOG.debug(f'{self.name} - Starting dispatch')

        self.msg_queue.start()
        self.dispatcher_glet = gevent.spawn(self._dispatcher)

    def start(self) -> None:
        assert self.state == ft_states.INIT

        self.set_state(ft_states.STARTED)

    def stop(self) -> None:
        assert self.state in [ft_states.STARTED, ft_states.IDLE]

        self.msg_queue.stop()
        if self.dispatcher_glet is not None:
            self.dispatcher_glet.kill()
            self.dispatcher_glet = None

        self.state = ft_states.STOPPED
