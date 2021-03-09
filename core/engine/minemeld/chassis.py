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
minemeld.chassis

A chassis instance contains a list of nodes and a fabric.
Nodes communicate using the fabric.
"""

import os
import logging
from typing import (
    Dict, Optional, List,
    Any, TYPE_CHECKING,
)

import gevent
import gevent.queue
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)

import minemeld.mgmtbus
import minemeld.ft
import minemeld.fabric

if TYPE_CHECKING:
    from minemeld.ft.base import BaseFT

LOG = logging.getLogger(__name__)
STATE_REPORT_INTERVAL = 10


class Chassis(object):
    """Chassis class

    Args:
        fabricclass (str): class for the fabric
        fabricconfig (dict): config dictionary for fabric,
            class specific
        mgmtbusconfig (dict): config dictionary for mgmt bus
    """
    def __init__(self, fabricclass: str, fabricconfig: dict, mgmtbusconfig: dict) -> None:
        self.chassis_id = os.getpid()

        self.fts: Dict[str, 'BaseFT'] = {}
        self.poweroff = gevent.event.AsyncResult()

        self.fabric_class = fabricclass
        self.fabric_config = fabricconfig
        self.fabric = minemeld.fabric.factory(
            self.fabric_class,
            self,
            self.fabric_config
        )

        self.mgmtbus = minemeld.mgmtbus.slave_hub_factory(
            mgmtbusconfig['slave'],
            mgmtbusconfig['transport']['class'],
            mgmtbusconfig['transport']['config']
        )
        self.mgmtbus.add_failure_listener(self.mgmtbus_failed)
        self.mgmtbus.request_chassis_rpc_channel(self)

        self.log_channel_queue = gevent.queue.Queue(maxsize=128)
        self.log_channel = self.mgmtbus.request_log_channel()
        self.log_glet = None

        self.status_channel_queue = gevent.queue.Queue(maxsize=128)
        self.status_glet = None

    def _dynamic_load(self, classname: str) -> 'BaseFT':
        modname, classname = classname.rsplit('.', 1)
        imodule = __import__(modname, globals(), locals(), [classname])
        cls = getattr(imodule, classname)
        return cls

    def get_ft(self, ftname: str) -> Optional['BaseFT']:
        return self.fts.get(ftname, None)

    def configure(self, config: dict)-> None:
        """configures the chassis instance

        Args:
            config (list): list of FTs
        """
        newfts: Dict[str,'BaseFT'] = {}
        for ft in config:
            ftconfig = config[ft]
            LOG.debug(ftconfig)

            # new FT
            newfts[ft] = minemeld.ft.factory(
                ftconfig['class'],
                name=ft,
                chassis=self,
                config=ftconfig.get('config', {})
            )
            newfts[ft].connect(
                ftconfig.get('inputs', []),
                ftconfig.get('output', False)
            )

        self.fts = newfts

        # XXX should be moved to constructor
        self.mgmtbus.start()
        self.fabric.start()

        self.mgmtbus.send_master_rpc(
            'chassis_ready',
            params={'chassis_id': self.chassis_id},
            timeout=10
        )

    def request_mgmtbus_channel(self, ft: 'BaseFT') -> None:
        self.mgmtbus.request_channel(ft)

    def request_pub_channel(self, ftname: str) -> Any:
        return self.fabric.request_pub_channel(ftname)

    def request_sub_channel(self, ftname: str, ft: 'BaseFT', subname: str, allowed_methods: Optional[List[str]]=None):
        if allowed_methods is None:
            allowed_methods = []
        self.fabric.request_sub_channel(ftname, ft, subname, allowed_methods)

    def _log_actor(self) -> None:
        while True:
            try:
                params = self.log_channel_queue.get()
                self.log_channel.publish(
                    method='log',
                    params=params
                )

            except Exception:
                LOG.exception('Error sending log')

    def log(self, timestamp: int, nodename: str, log_type: str, value: dict) -> None:
        self.log_channel_queue.put({
            'timestamp': timestamp,
            'source': nodename,
            'log_type': log_type,
            'log': value
        })

    def _status_actor(self) -> None:
        while True:
            try:
                params = self.status_channel_queue.get()
                self.mgmtbus.send_status(
                    params=params
                )

            except Exception:
                LOG.exception('Error publishing status')

    def publish_status(self, timestamp: int, nodename: str, status: dict) -> None:
        self.status_channel_queue.put({
            'timestamp': timestamp,
            'source': nodename,
            'status': status
        })

    def fabric_failed(self) -> None:
        self.stop()

    def _nodes_rpc(self, command: str, target: Optional[str], **kwargs) -> Dict[str,Any]:
        if target == "<chassis>" or target is None:
            LOG.error(f"state_info received with target {target}")
            return {}

        nodes: Dict[str,'BaseFT'] = self.fts
        if target != "<nodes>":
            if target not in self.fts:
                return {}

            nodes = {}
            nodes[target] = self.fts[target]

        result: Dict[str,Any] = {}
        for nodename, node in nodes.items():
            m = getattr(node, command, None)
            if m is None:
                raise RuntimeError(f"{self.chassis_id} - method {command} not found for {nodename}")

            result[nodename] = m(**kwargs)

        return result

    def mgmtbus_failed(self) -> None:
        LOG.critical('chassis - mgmtbus failed')
        self.stop()

    def mgmtbus_start(self, target: Optional[str]=None) -> str:
        if target != "<chassis>":
            LOG.error(f"start received with target {target}")
            return 'ok'

        LOG.info('chassis - start received from mgmtbus')
        self.start()
        return 'ok'

    def mgmtbus_state_info(self, target: Optional[str]=None) -> Dict[str,dict]:
        return self._nodes_rpc('mgmtbus_state_info',target)

    def mgmtbus_initialize(self, target: Optional[str]=None) -> Dict[str,str]:
        return self._nodes_rpc('mgmtbus_initialize',target)

    def mgmtbus_rebuild(self, target: Optional[str]=None) -> Dict[str,str]:
        return self._nodes_rpc('mgmtbus_rebuild',target)

    def mgmtbus_reset(self, target: Optional[str]=None) -> Dict[str,str]:
        return self._nodes_rpc('mgmtbus_reset',target)

    def mgmtbus_status(self, target: Optional[str]=None) -> Dict[str,dict]:
        return self._nodes_rpc('mgmtbus_status',target)

    def mgmtbus_checkpoint(self, target: Optional[str]=None, value: Optional[str]=None) -> Dict[str,str]:
        return self._nodes_rpc('mgmtbus_checkpoint', target, value=value)

    def mgmtbus_hup(self, target: Optional[str]=None, source: Optional[str]=None) -> Dict[str,None]:
        return self._nodes_rpc('mgmtbus_hup',target, source=source)

    def mgmtbus_signal(self, target: Optional[str]=None, signal: Optional[str]=None, **kwargs) -> Dict[str,dict]:
        return self._nodes_rpc('mgmtbus_signal',target)

    def fts_init(self) -> bool:
        for ft in self.fts.values():
            if ft.state < minemeld.ft.ft_states.INIT:
                return False
        return True

    def sig_stop(self, signum: int, sigstack: Any) -> None:
        gevent.spawn(self.stop)

    def stop(self) -> None:
        LOG.info("chassis stop called")

        if self.log_glet is not None:
            self.log_glet.kill()

        if self.status_glet is not None:
            self.status_glet.kill()

        if self.fabric is None:
            return

        for ftname, ft in self.fts.items():
            try:
                ft.stop()
            except:
                LOG.exception('Error stopping {}'.format(ftname))

        LOG.info('Stopping fabric')
        self.fabric.stop()

        LOG.info('Stopping mgmtbus')
        self.mgmtbus.stop()

        LOG.info('chassis - stopped')
        self.poweroff.set(value='stop')

    def start(self) -> None:
        LOG.info("chassis start called")

        self.log_glet = gevent.spawn(self._log_actor)
        self.status_glet = gevent.spawn(self._status_actor)

        for ftname, ft in self.fts.items():
            LOG.debug("starting %s", ftname)
            ft.start()

        self.fabric.start_dispatching()
