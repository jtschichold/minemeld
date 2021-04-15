from enum import IntEnum, Enum
from typing import (
    Protocol, Optional, TYPE_CHECKING,
    Dict, Any, List, TypedDict
)

import gevent.event

from minemeld.loader import load, MM_NODES_ENTRYPOINT
from minemeld.comm.pubsub import Publisher

if TYPE_CHECKING:
    from minemeld.chassis import Chassis


def factory(classname: str, name: str, chassis: 'Chassis', num_inputs: int):
    node_class = load(MM_NODES_ENTRYPOINT, classname)

    return node_class(
        name=name,
        chassis=chassis,
        num_inputs=num_inputs
    )


class FTState(IntEnum):
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
    state: FTState
    def __init__(self, name: str, chassis: 'Chassis', num_inputs: int):
        ...

    def configure(self, config: Dict[str, Any]) -> None:
        ...

    def connect(self, p: Publisher) -> None:
        ...

    def start_dispatch(self) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def on_rpc_reactor_msg(self, method: str, source: Optional[str] = None, **kwargs) -> Optional[gevent.event.AsyncResult]:
        ...

    def on_pubsub_reactor_msg(self, method: str, source: Optional[str] = None, **kwargs) -> bool:
        ...


class ValidateResult(TypedDict, total=False):
    errors: List[str]
    requires_reinit: bool


class NodeType(Enum):
    MINER = 1
    PROCESSOR = 2
    OUTPUT = 3


class MetadataResult(TypedDict):
    node_type: NodeType


class MetadataNode(Protocol):
    @staticmethod
    def get_metadata() -> MetadataResult:
        ...

    @staticmethod
    def get_schema() -> Dict[str, Any]:
        ...

    @staticmethod
    def validate(newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidateResult:
        ...
