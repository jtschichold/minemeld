import logging
import json
from random import shuffle
from typing import (
    Optional, Any, Dict, Union, List, Callable
)
from hashlib import sha1
from collections import defaultdict

import gevent
import redis


LOG = logging.getLogger(__name__)


PUBLISH = b"""
local function publish (queue, subscribers, msg_number, msg)
    redis.call(
        'rpush', queue, msg
    )
    local num_subscribers = redis.call('llen', subscribers)

    for s=0,(num_subscribers-1) do
        local subscriber_wl = subscribers .. ':' .. tostring(s)
        redis.call(
            'lpush',
            subscriber_wl,
            msg_number
        )
        redis.call(
            'ltrim',
            subscriber_wl,
            0, 0
        )
    end

    return redis.status_reply("OK")
end

return publish(ARGV[1],ARGV[2],ARGV[3],ARGV[4])
"""
PUBLISH_SHA1 = sha1(PUBLISH).hexdigest()


class Publisher:
    redis_client: Optional[redis.Redis]

    def __init__(self, topic: str, connection_pool: redis.ConnectionPool):
        self.topic = topic
        self.prefix = 'mm:topic:{}'.format(self.topic)
        self.connection_pool = connection_pool
        self.redis_client = None
        self.num_publish = 0
        self.publish_sha1 = PUBLISH_SHA1

    def lagger(self) -> int:
        assert self.redis_client is not None

        # get status of subscribers
        subscribersc: List[bytes] = self.redis_client.lrange(
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
        assert self.redis_client is not None

        minhighbits = lagger >> 12

        minqname = '{}:queue:{:013X}'.format(
            self.prefix,
            minhighbits
        )

        # delete all the lists before the lagger
        queues: List[bytes] = self.redis_client.keys(f'{self.prefix}:queue:*')
        LOG.debug('topic {} - queues: {!r}'.format(self.topic, queues))
        queues = [q for q in queues if q.decode('utf-8') < minqname]
        LOG.debug(
            'topic {} - queues to be deleted: {!r}'.format(self.topic, queues))
        if len(queues) != 0:
            LOG.debug('topic {} - deleting {!r}'.format(
                self.topic,
                queues
            ))
            self.redis_client.delete(*queues)

    def publish(self, method: str, params: Optional[Dict[str, Union[str, int, bool]]] = None) -> None:
        assert self.redis_client is not None

        high_bits = self.num_publish >> 12
        low_bits = self.num_publish & 0xfff

        if (low_bits % 128) == 127:
            lagger = self.lagger()
            LOG.debug('topic {} - sent {} lagger {}'.format(
                self.topic,
                self.num_publish,
                lagger
            ))

            wait_time = 1
            while (self.num_publish - lagger) > 1024:
                LOG.debug('topic {} - waiting lagger delta: {}'.format(
                    self.topic,
                    self.num_publish - lagger
                ))
                gevent.sleep(0.01 * wait_time)

                wait_time = wait_time * 2
                if wait_time > 100:
                    wait_time = 100

                lagger = self.lagger()

            if low_bits == 0xfff:
                # we are switching to a new list, gc
                self.gc(lagger)

        msg = {
            'method': method,
            'params': params
        }

        self.redis_client.evalsha(
            self.publish_sha1,
            0,
            f'{self.prefix}:queue:{high_bits:013X}',
            f'{self.prefix}:subscribers',
            self.num_publish,
            json.dumps(msg)
        )

        self.num_publish += 1

    def connect(self):
        if self.redis_client is not None:
            return

        self.redis_client = redis.Redis(
            connection_pool=self.connection_pool
        )

        scripts_exist: List[bool] = self.redis_client.script_exists(
            PUBLISH_SHA1
        )
        if not scripts_exist[0]:
            self.publish_sha1 = self.redis_client.script_load(PUBLISH)
            if self.publish_sha1 != PUBLISH_SHA1:
                raise RuntimeError(
                    f'Redis calculated SHA1 for publish script differs from our value! {self.publish_sha1} vs {PUBLISH_SHA1}')

    def disconnect(self):
        self.redis_client = None
        self.publish_sha1 = PUBLISH_SHA1


class Subscriber:
    sub_number: Optional[int]
    redis_client: Optional[redis.Redis]

    def __init__(self, topic: str, connection_pool: redis.ConnectionPool, handler: Callable[[bytes], None]):
        self.topic = topic
        self.prefix = 'mm:topic:{}'.format(self.topic)
        self.channel = None
        self.connection_pool = connection_pool
        self.handler = handler

        self.sub_number = None
        self.redis_client = None

        self.counter = 0
        self.subscribers_key = '{}:subscribers'.format(self.prefix)
        self.subscriber_list: Optional[str] = None

    def connect(self):
        if self.redis_client is None:
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)

        if self.sub_number is None:
            self.sub_number = self.redis_client.rpush(
                self.subscribers_key,
                0
            )
            self.sub_number -= 1
            self.subscriber_list = f'{self.subscribers_key}:{self.sub_number}'.encode('utf-8')
            LOG.debug('Sub Number {} on {}'.format(
                self.sub_number, self.subscribers_key))

    def disconnect(self):
        self.redis_client = None
        self.sub_number = None
        self.subscriber_list = None

    def consume(self) -> bool:
        assert self.redis_client is not None
        assert self.sub_number is not None

        base = self.counter & 0xfff
        top = min(base + 127, 0xfff)

        msgs = self.redis_client.lrange(
            '{}:queue:{:013X}'.format(self.prefix, self.counter >> 12),
            base,
            top
        )

        for m in msgs:
            self.handler(m)

        self.counter += len(msgs)

        if len(msgs) > 0:
            self.redis_client.lset(
                self.subscribers_key,
                self.sub_number,
                self.counter
            )

        return len(msgs) > 0


class Reactor:
    def __init__(self, connection_pool: redis.ConnectionPool):
        self.connection_pool = connection_pool
        self.redis_client: Optional[redis.Redis] = None
        self.publishers: List[Publisher] = []
        self.subscribers: List[Subscriber] = []

    def new_publisher(self, topic: str) -> Publisher:
        p = Publisher(
            topic,
            self.connection_pool
        )
        self.publishers.append(p)

        return p

    def new_subscriber(self, topic: str, connection_pool: redis.ConnectionPool, handler: Callable[[bytes], None]) -> Subscriber:
        s = Subscriber(
            topic,
            connection_pool,
            handler
        )
        self.subscribers.append(s)

        return s

    def select(self) -> Optional[Subscriber]:
        if self.redis_client is None:
            self.redis_client = redis.Redis(
                connection_pool=self.connection_pool
            )

        sublists: Dict[str, Subscriber] = {s.subscriber_list: s for s in self.subscribers if s.subscriber_list is not None}
        if len(sublists) == 0:
            return None

        while True:
            keys = list(sublists.keys())
            shuffle(keys)
            ready = self.redis_client.blpop(keys=keys, timeout=0)
            if ready is None:
                continue

            if ready[0] not in sublists:
                LOG.warning(f'Unknown sublist: {ready[0]}')
                continue

            if sublists[ready[0]].counter > int(ready[1].decode('utf-8')):
                LOG.debug(f'Old counter on {ready[0]} - ignored')
                continue

            return sublists[ready[0]]

    def run(self):
        # for testing
        while True:
            ready_subscriber = self.select()
            LOG.debug(f'Selector: ready {ready_subscriber.subscriber_list}')
            if ready_subscriber is None:
                return

            while ready_subscriber.consume():
                pass

    def connect(self):
        for s in self.subscribers:
            s.connect()

        for p in self.publishers:
            p.connect()

    def disconnect(self):
        for s in self.subscribers:
            s.disconnect()

        for p in self.publishers:
            p.disconnect()

    @staticmethod
    def cleanup(connection_pool: redis.ConnectionPool):
        redis_client: Optional[redis.Redis] = redis.Redis(connection_pool=connection_pool)
        assert redis_client is not None

        tkeys: List[bytes] = redis_client.keys(pattern='mm:topic:*')
        if len(tkeys) > 0:
            LOG.info('Deleting old keys: {}'.format(len(tkeys)))
            redis_client.delete(*tkeys)

        redis_client = None
