import gevent
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)

import logging

import redis
import zmq.green as zmq

import pubsub
import rpc

# pubsub testing
class Counter:
    def __init__(self, n):
        self.n = n
        self._count = 0

    def count(self, x):
        self._count += 1

    def __str__(self):
        return f'{self.n}: {self._count}'

def pf(publisher):
    for i in range(1000000):
        publisher.publish(method="show", params={"n": i})
    print('XXX - PF Done !!!')

def rpcf(rpcclient: rpc.Client):
    for i in range(10000):
        rpcclient.send_rpc(
            method="count",
            params={
                'x': i
            },
            num_results=1
        )
        gevent.sleep(0.001)
    print('XXX - RPC Done!!!')

logging.basicConfig(level=logging.INFO)

connection_pool = redis.ConnectionPool()
pubsub.Reactor.cleanup(connection_pool)

reactor = pubsub.Reactor(connection_pool)
counter1 = Counter("c1:t1")
counter2 = Counter("c2:t1")
counter3 = Counter("c:t2")
subscriber1 = reactor.new_subscriber(
    topic="t1",
    connection_pool=connection_pool,
    handler=counter1.count
)
subscriber2 = reactor.new_subscriber(
    topic="t1",
    connection_pool=connection_pool,
    handler=counter2.count
)
subscriber3 = reactor.new_subscriber(
    topic="t2",
    connection_pool=connection_pool,
    handler=counter3.count
)
publisher1 = reactor.new_publisher(topic="t1")
publisher2 = reactor.new_publisher(topic="t2")

context = zmq.Context()
rpcreactor = rpc.Reactor(
    path='/tmp',
    context=context
)
rpccount = Counter('rpcc1')
rpcclient = rpcreactor.new_client(fanout='mm-test')
rpcserver = rpcreactor.new_server(
    name='test',
    obj=rpccount,
    allowed_methods=['count'],
    fanout='mm-test'
)

reactor.connect()
rpcreactor.connect()

r = gevent.spawn(reactor.run)
r2 = gevent.spawn(rpcreactor.run)
gevent.joinall([
    gevent.spawn(rpcf, rpcclient),
    gevent.spawn(pf, publisher1),
    gevent.spawn(pf, publisher2)
])
gevent.sleep(60.0)

reactor.disconnect()
rpcreactor.disconnect()
context.destroy()

print(str(counter1))
print(str(counter2))
print(str(counter3))
print(str(rpccount))

