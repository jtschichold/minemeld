#  Copyright 2015-2016 Palo Alto Networks, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
minemeld.fabric

This module implements fabric abstraction over communication backend class.
Each chassis has an instance of Fabric and nodes request connections to the
fabric using this instance.
"""

import logging
from typing import (
    List, Union, Dict,
    TYPE_CHECKING
)

import minemeld.comm
if TYPE_CHECKING:
    from minemeld.comm.zmqredis import RedisPubChannel
    from minemeld.chassis import Chassis
    from minemeld.ft.base import BaseFT

LOG = logging.getLogger(__name__)


class Fabric(object):
    """MineMeld chassis fabric class

    Args:
        chassis: MineMeld chassis instance
        config (dict): communication backend config
        comm_class (string): communication backend to be used
    """
    def __init__(self, chassis: 'Chassis', config: dict, comm_class: str) -> None:
        self.chassis = chassis

        self.comm_config = config
        self.comm_class = comm_class

        self.comm = minemeld.comm.factory(self.comm_class, self.comm_config)

    def request_pub_channel(self, ftname: str) -> 'RedisPubChannel':
        """Creates a new channel for publishing to a topic with name ftname.

        Args:
            ftname (str): node name
        """
        return self.comm.request_pub_channel(ftname) # type: ignore

    def request_sub_channel(self, ftname: str, node: 'BaseFT', subname: str, allowed_methods: List[str]) -> None:
        """Creates a subscription channel to topic subname.

        Args:
            ftname (str): name of the node
            node: node instance
            subname (str): name of the topic to subscribe to
            allowed_methods (list): list of allowed methods
        """
        _ = ftname  # noqa
        self.comm.request_sub_channel(subname, node, allowed_methods)

    def _comm_failure(self) -> None:
        self.chassis.fabric_failed()

    def start(self) -> None:
        LOG.debug("fabric start called")
        self.comm.add_failure_listener(self._comm_failure)
        self.comm.start(start_dispatching=False)

    def start_dispatching(self) -> None:
        self.comm.start_dispatching()

    def stop(self) -> None:
        LOG.debug("fabric stop called")
        self.comm.stop()


def factory(classname: str, chassis: 'Chassis', config: dict):
    """Factory for Fabric class.

    Args:
        classname (str): communication backend name
        chassis: chassis instance
        config (dict): communication backend config
    """
    return Fabric(
        chassis=chassis,
        config=config,
        comm_class=classname
    )
