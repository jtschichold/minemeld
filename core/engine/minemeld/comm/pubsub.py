import array
import logging
import json
import time
import struct
import random
from itertools import chain
from typing import (
    Optional, Any, Dict, Union, List, Callable,
    Tuple, TYPE_CHECKING, cast, Sequence, Protocol,
    Mapping
)
from collections import defaultdict, deque
from multiprocessing.shared_memory import SharedMemory

import gevent
import gevent.event
import msgpack


PAGE_SIZE = 4 * 1024
PUB_QUEUE_MAXLEN = 1024
PUB_QUEUE_ENTRY_MAXLEN = 2 * PAGE_SIZE
PUB_MAX_SUBSCRIBERS = (PUB_QUEUE_ENTRY_MAXLEN // 4) - 1
LOG = logging.getLogger(__name__)


class PubSubHandler(Protocol):
    def __call__(self, method, **kwargs) -> Any:
        ...


class CircularBuffer:
    def __init__(self, name: str):
        self.name = name
        self.owned: bool = False
        self.shm: Optional[SharedMemory] = None
        self.subscribers_shm: Optional[memoryview] = None

    def create(self) -> None:
        assert self.shm is None

        try:
            temp_shm = SharedMemory(name=self.name, create=False)
            temp_shm.unlink()
            temp_shm.close()

        except FileNotFoundError:
            pass

        self.shm = SharedMemory(
            name=self.name,
            create=True,
            size=(PUB_QUEUE_ENTRY_MAXLEN * PUB_QUEUE_MAXLEN + (PUB_MAX_SUBSCRIBERS * 4) + 4)
        )
        self.owned = True
        self.subscribers_shm = self.shm.buf.cast('I')

    def load(self) -> None:
        assert self.shm is None

        self.shm = SharedMemory(
            name=self.name,
            create=False
        )
        self.owned = False
        self.subscribers_shm = self.shm.buf.cast('I')

    def write_next(self) -> int:
        assert self.subscribers_shm is not None

        return self.subscribers_shm[0]

    def set_write_next(self, n: int) -> None:
        assert self.subscribers_shm is not None

        self.subscribers_shm[0] = n  # type: ignore

    def read_next(self, subscriber: int) -> int:
        assert self.subscribers_shm is not None

        return self.subscribers_shm[1 + subscriber]

    def set_read_next(self, subscriber: int, n: int) -> None:
        assert self.subscribers_shm is not None

        self.subscribers_shm[1 + subscriber] = n  # type: ignore

    def queue_entry_limits(self, n, msg_size = None) -> Tuple[int, int]:
        if msg_size is None:
            msg_size = PUB_QUEUE_ENTRY_MAXLEN
        offset = (PUB_MAX_SUBSCRIBERS * 4) + 4 + n * PUB_QUEUE_ENTRY_MAXLEN
        return (
            offset,
            offset + msg_size
        )

    def dispose(self):
        if self.subscribers_shm is not None:
            self.subscribers_shm.release()
            self.subscribers_shm = None

        if self.shm is not None:
            self.shm.close()
            if self.owned:
                self.shm.unlink()
            self.shm = None

        self.owned = False


# The layout of the shared memory
# +--next--+---- subscribers counters (PUB_MAX_SUBSCRIBERS * 4) ---+-- queue (PUB_QUEUE_ENTRY_MAXLEN * PUB_QUEUE_MAXLEN) --+
# Each subscriber counter is a 4 bytes integer
# Each element in the queue is PUB_QUEUE_ENTRY_MAXLEN
class Publisher:
    def __init__(self, topic: str, num_subscribers: int):
        if num_subscribers > PUB_MAX_SUBSCRIBERS:
            raise RuntimeError('Too many subscribers!')

        self.topic = topic
        self.num_subscribers = num_subscribers
        self.counter = 0
        self.circular_buffer: Optional[CircularBuffer] = None

        self.wait_slots = 1
        self.next_check = 0.0
        self.waiting: Optional[gevent.event.Event] = None

    def publish(self, method: str, params: Optional[Mapping[str, Union[str, int, bool, dict, list]]] = None) -> None:
        assert self.circular_buffer is not None
        assert self.circular_buffer.shm is not None
        assert self.circular_buffer.subscribers_shm is not None

        while self.full():
            self.next_check = time.time() + self.wait_slots * 0.01
            self.waiting = gevent.event.Event()
            self.waiting.wait()
        self.waiting = None
        self.wait_slots = 1

        cnext = self.circular_buffer.write_next()
        msg_encoded = msgpack.packb({
            "method": method,
            "params": params
        })
        if len(msg_encoded) > PUB_QUEUE_ENTRY_MAXLEN:
            raise RuntimeError(f'Msg not sent, too big: {len(msg_encoded)}')
        boundaries = self.circular_buffer.queue_entry_limits(cnext, len(msg_encoded))
        self.circular_buffer.shm.buf[boundaries[0] : boundaries[1]] = msg_encoded

        self.circular_buffer.set_write_next((cnext + 1) % PUB_QUEUE_MAXLEN)
        self.counter += 1

    def full(self) -> bool:
        assert self.circular_buffer is not None
        assert self.circular_buffer.subscribers_shm is not None

        full_value = (self.circular_buffer.write_next() + 1) % PUB_QUEUE_MAXLEN
        for sn in range(0, self.num_subscribers):
            if self.circular_buffer.subscribers_shm[sn + 1] == full_value:
                return True
        return False

    def connect(self, circular_buffer: CircularBuffer):
        assert circular_buffer.subscribers_shm is not None

        self.circular_buffer = circular_buffer

        assert self.circular_buffer.subscribers_shm is not None
        for i in range(self.num_subscribers + 1):
            self.circular_buffer.subscribers_shm[i] = 0  # type: ignore

    def disconnect(self):
        self.circular_buffer = None


class Subscriber:
    def __init__(self, topic: str, subscriber_number: int, handler: PubSubHandler):
        self.topic = topic
        self.subscriber_number = subscriber_number
        self.handler: PubSubHandler = handler
        self.next_check = 0.0
        self.wait_slots = 1
        self.circular_buffer: Optional[CircularBuffer] = None

    def connect(self, circular_buffer: CircularBuffer) -> None:
        self.circular_buffer = circular_buffer

    def disconnect(self):
        self.circular_buffer = None

    def ready(self) -> bool:
        assert self.circular_buffer is not None

        return self.circular_buffer.write_next() != self.circular_buffer.read_next(self.subscriber_number)

    def consume(self) -> bool:
        assert self.circular_buffer is not None
        assert self.circular_buffer.shm is not None
        assert self.circular_buffer.subscribers_shm is not None
    
        cnext = self.circular_buffer.write_next()
        snext = self.circular_buffer.read_next(self.subscriber_number)

        if cnext == snext:
            # empty
            self.next_check = time.time() + self.wait_slots * 0.01
            self.wait_slots = min(100, self.wait_slots * 2)
            return False

        # not empty, reset waiting state
        self.wait_slots = 1

        ranges = []
        if snext < cnext:
            ranges.append(range(snext, cnext))
        else:
            ranges.append(range(snext, PUB_QUEUE_MAXLEN))
            ranges.append(range(cnext))
        
        count = 0
        for mn in chain(*ranges):
            count += 1
            unpacker = msgpack.Unpacker()
            try:
                boundaries = self.circular_buffer.queue_entry_limits(mn)
                unpacker.feed(self.circular_buffer.shm.buf[boundaries[0] : boundaries[1]])
                if (msg := next(unpacker, None)) is None:
                    LOG.error('No message')
                    continue
                
                method = msg.get('method', None)
                if method is None:
                    LOG.error(f'No method attribute in message')
                    continue

                params = msg.get('params', {})

                LOG.debug(f'PubSub Subscriber {self.topic} - handling msg {method}/{params}')
                self.handler(method, **params)

            except gevent.GreenletExit:
                raise

            except Exception:
                LOG.error('Error handling message: ', exc_info=True)

        LOG.debug(f'consumed: {count}')

        self.circular_buffer.set_read_next(self.subscriber_number, cnext)
        self.next_check = time.time() + self.wait_slots * 0.01

        return True


class Reactor:
    def __init__(self, path: str, topics: List[Tuple[str,Optional[int]]]):
        self.publishers: List[Publisher] = []
        self.subscribers: List[Subscriber] = []
        self.path = path
        self.topics = topics
        self.circular_buffers: Dict[str, CircularBuffer] = {}
        self.initialize_circular_buffers(topics)

    def initialize_circular_buffers(self, topics: List[Tuple[str,Optional[int]]]):
        LOG.debug(f'topics: {topics}')
        for topic, num_subscribers in topics:
            if num_subscribers is None:
                self.circular_buffers[topic] = CircularBuffer(
                    name=topic
                )
            else:
                self.circular_buffers[topic] = CircularBuffer(
                    name=topic
                )
                self.circular_buffers[topic].create()

    def new_publisher(self, topic: str) -> Publisher:
        if (topic_info := next((t for t in self.topics if t[0] == topic), None)) is None:
            raise RuntimeError(f'Unknown topic {topic}')
        if topic_info[1] is None:
            raise RuntimeError(f'Publisher requested for subscriber topic {topic}')
        
        p = Publisher(
            topic=topic,
            num_subscribers=topic_info[1]
        )
        self.publishers.append(p)

        return p

    def new_subscriber(self, subscriber_number: int, topic: str, handler: PubSubHandler) -> Subscriber:
        if next((t for t in self.topics if t[0] == topic), None) is None:
            raise RuntimeError(f'Unknown topic {topic}')

        s = Subscriber(
            topic=topic,
            subscriber_number=subscriber_number,
            handler=handler
        )
        self.subscribers.append(s)

        return s

    def wait_interval(self) -> Optional[float]:
        LOG.debug(f'PubSub Reactor - wait interval')
        waiting_publishers = [p.next_check for p in self.publishers if p.waiting is not None]
        if len(self.subscribers) == 0 and len(waiting_publishers) == 0:
            return None

        now = time.time()
        next_check = min(
            [s.next_check for s in self.subscribers] + waiting_publishers
        )
        return max(0.0, next_check - now)

    def dispatch(self) -> None:
        now = time.time()
        waiting_subscribers = [s for s in self.subscribers if s.next_check <= now]
        if len(waiting_subscribers) != 0:
            random.shuffle(waiting_subscribers)
            for s in waiting_subscribers:
                if s.consume():
                    return

        waiting_publishers = [p for p in self.publishers if p.waiting is not None and p.next_check <= now and not p.waiting.is_set()]
        if len(waiting_publishers) != 0:
            p = random.choice(waiting_publishers)
            assert p.waiting is not None
            p.waiting.set()

    def connect(self):
        for p in self.publishers:
            p.connect(self.circular_buffers[p.topic])

        for cb in self.circular_buffers.values():
            if cb.owned:
                continue
            cb.load()

        for s in self.subscribers:
            s.connect(self.circular_buffers[s.topic])

    def disconnect(self):
        for s in self.subscribers:
            s.disconnect()
        
        for p in self.publishers:
            p.disconnect()

        for cb in self.circular_buffers.values():
            cb.dispose()
