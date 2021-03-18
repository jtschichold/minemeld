from typing import (
    Optional, Dict, Any
)

import gevent
import gevent.queue

from minemeld.chassis import Chassis
from minemeld.comm.pubsub import Publisher

from . import ft_states, ChassisNode
from .utils import utc_millisec, GThrottled

class BaseFT:
    def __init__(self, name: str, chassis: Chassis):
        self.name = name
        self.chassis = chassis
        self.state = ft_states.READY

        self.msg_queue: gevent.queue.PriorityQueue = gevent.queue.PriorityQueue(maxsize=2)
        self.dispatcher_glet: Optional[gevent.Greenlet] = None

        self._last_status_publish: Optional[float] = None
        self._throttled_publish_status = GThrottled(self._internal_publish_status, 3000)
        self._clock = 0

        self.statistics: Dict[str, int] = {}

        self.publisher: Optional[Publisher] = None

    def set_state(self, new_state: int) -> None:
        self.state = new_state
        self.publish_status(force=True)

    def connect(self, p: Publisher) -> None:
        self.publisher = p

    def publish_status(self, force=False):
        if force:
            self._internal_publish_status()

        self._throttled_publish_status()

    def _internal_publish_status(self):
        self._last_status_publish = utc_millisec()
        status = self.status()
        self.chassis.publish_status(
            timestamp=self._last_status_publish,
            nodename=self.name,
            status=status
        )

    def status(self) -> Dict[str, Any]:
        result = {
            'clock': self._clock,
            'class': (self.__class__.__module__+'.'+self.__class__.__name__),
            'state': self.state,
            'statistics': self.statistics,
        }
        self._clock += 1
        return result

    def _dispatcher(self):
        while True:
            gevent.wait()

    def start(self) -> None:
        assert self.state == ft_states.INIT
        self.dispatcher_glet = gevent.spawn(self._dispatcher)
        self.set_state(ft_states.STARTED)

    def stop(self) -> None:
        assert self.state in [ft_states.STARTED, ft_states.INIT]

        self.state = ft_states.STOPPED
