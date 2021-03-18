from typing import Protocol, Optional

from minemeld.chassis import RPCNodeAsyncAnswer
from minemeld.loader import load, MM_NODES_ENTRYPOINT
from minemeld.comm.pubsub import Publisher


def factory(classname, name, chassis):
    node_class = load(MM_NODES_ENTRYPOINT, classname)

    return node_class(
        name=name,
        chassis=chassis
    )


class ft_states:
    READY = 0
    CONNECTED = 1
    REBUILDING = 2
    RESET = 3
    INIT = 4
    STARTED = 5
    CHECKPOINT = 6
    IDLE = 7
    STOPPED = 8


class ChassisNode(Protocol):
    state: int
    def connect(self, p: Publisher) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def on_msg(self, method: str, **kwargs) -> Optional[RPCNodeAsyncAnswer]:
        ...
