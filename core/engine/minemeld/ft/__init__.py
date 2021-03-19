from typing import Protocol, Optional

from minemeld.chassis import RPCNodeAsyncAnswer
from minemeld.loader import load, MM_NODES_ENTRYPOINT
from minemeld.comm.pubsub import Publisher
from minemeld.config import TMineMeldNodeConfig
from minemeld.chassis import Chassis


def factory(classname: str, name: str, chassis: Chassis, num_inputs: int):
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
    def __init__(self, name: str, chassis: Chassis, num_inputs: int):
        ...

    def configure(self, config: TMineMeldNodeConfig) -> None:
        ...

    def connect(self, p: Publisher) -> None:
        ...

    def start_dispatch(self) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def on_msg(self, method: str, **kwargs) -> Optional[RPCNodeAsyncAnswer]:
        ...
