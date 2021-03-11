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
This module implements minemeld.ft.base.BaseFT, the base class for nodes.
"""



import logging
import copy
import os
import collections
import json
from typing import (
    Optional, Union, List,
    DefaultDict, Dict, Tuple,
    TypeVar, Callable, Any,
    TYPE_CHECKING
)

import gevent

from . import filters
from . import ft_states
from . import utils

if TYPE_CHECKING:
    from minemeld.chassis import Chassis

LOG = logging.getLogger(__name__)

CountedF = TypeVar('CountedF', bound=Callable[..., Any])


def _counting(statsname: str) -> Callable[[CountedF], CountedF]:
    """Decorator for counting calls to decorated instance methods.
    Counters are stored in statistics attribute of the instance.

    Args:
        statsname (str): name of the counter to increment
    """
    def _counter_out(f):
        def _counter(self, *args, **kwargs):
            self.statistics[statsname] += 1
            f(self, *args, **kwargs)
            self.publish_status()
        return _counter
    return _counter_out


class BaseFT(object):
    """Implements base class of MineMeld engine nodes.

    **Config parameters**

        :infilters: inbound filter set. Filters to be applied to
            received indicators.
        :outfilters: outbound filter set. Filters to be applied to
            transmitted indicators.

    **Filter set**
        Each filter set is a list of filters. Filters are verified from top
        to bottom, and the first matching filter is applied. Default action
        is **accept**.
        Each filter is a dictionary with 3 keys:

        :name: name of the filter.
        :conditions: list of boolean expressions to match on the
            indicator.
        :actions: list of actions to be applied to the indicator.
            Currently the only supported actions are **accept** and **drop**

        In addition to the atttributes in the indicator value, filters can
        match on 3 special attributes:

        :__indicator: the indicator itself.
        :__method: the method of the message, **update** or **withdraw**.
        :__origin: the name of the node who sent the indicator.

    **Condition**
        A condition in the filter, is boolean expression composed by a JMESPath
        expression, an operator (<, <=, ==, >=, >, !=) and a value.

    Example:
        Example config in YAML::

            infilters:
                - name: accept withdraws
                  conditions:
                    - __method == 'withdraw'
                  actions:
                    - accept
                - name: accept URL
                  conditions:
                    - type == 'URL'
                  actions:
                    - accept
                - name: drop all
                  actions:
                    - drop
            outfilters:
                - name: accept all (default)
                  actions:
                    - accept

    Args:
        name (str): node name, should be unique inside the graph
        chassis (object): parent chassis instance
        config (dict): node config.
    """

    last_checkpoint: Optional[str]

    def __init__(self, name: str, chassis: 'Chassis', config: dict):
        self.name = name

        self.chassis = chassis

        self._original_config = copy.deepcopy(config)
        self.config = config
        self.configure()

        self.inputs: List[str] = []
        self.output = None

        self.statistics: DefaultDict[str, int] = collections.defaultdict(int)

        self.read_checkpoint()

        self.chassis.request_mgmtbus_channel(self)

        self._state = ft_states.READY

        self._last_status_publish = None
        self._throttled_publish_status = utils.GThrottled(self._internal_publish_status, 3000)
        self._clock = 0

        self._disable_full_trace = 'MM_DISABLE_FULL_TRACE' in os.environ
        self._disable_full_trace_glet = None

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int):
        LOG.info("%s - transitioning to state %d", self.name, value)
        self._state = value

        if value >= ft_states.INIT and value <= ft_states.STOPPED:
            self.publish_status(force=True)

    def read_checkpoint(self) -> None:
        """Reads checkpoint file from disk.

        First line of the checkpoint file is a UUID, the *checkpoint* received
        before stopping. The second line is a dictionary in JSON with the class
        of the node and the config. The third line is a dictionary in JSON
        with the persistent state of the node.

        Checkpoint files are used to check if the saved state on disk is
        consistent with the current running config. If the state is not
        consistent `last_checkpoint` is set to None, to indicate that the state
        stored on disk is not valid or inexistent.

        Called by `__init__`.
        """
        self.last_checkpoint = None

        config = {
            'class': (self.__class__.__module__+'.'+self.__class__.__name__),
            'config': self._original_config
        }

        try:
            with open(self.name+'.chkp', 'r') as f:
                contents = f.read()
                if contents[0] == '{':
                    # new format
                    decoded_contents = json.loads(contents)
                    self.last_checkpoint = decoded_contents['checkpoint']
                    saved_config = decoded_contents['config']
                    saved_state = decoded_contents['state']

                else:
                    # old format
                    lines = contents.splitlines()
                    self.last_checkpoint = lines[0]

                    saved_config = ''
                    if len(lines) > 1:
                        # this to support a really old format
                        # where only checkpoint value was saved
                        saved_config = lines[1]

                    saved_state = None

                LOG.debug('%s - restored checkpoint: %s', self.name, self.last_checkpoint)

            # old_status is missing in old releases
            # stick to the old behavior
            if saved_config and saved_config != json.dumps(config, sort_keys=True):
                LOG.info(
                    '%s - saved config does not match new config',
                    self.name
                )
                self.last_checkpoint = None
                return

            LOG.info(
                '%s - saved config matches new config',
                self.name
            )

            if saved_state is not None:
                self._saved_state_restore(saved_state)

        except (ValueError, IOError):
            LOG.exception('%s - Error reading last checkpoint', self.name)
            self.last_checkpoint = None

    def create_checkpoint(self, value: str) -> None:
        """Saves checkpoint file to disk.

        Called by `checkpoint`.

        Args:
            value (str): received *checkpoint*
        """
        config = {
            'class': (self.__class__.__module__+'.'+self.__class__.__name__),
            'config': self._original_config
        }

        contents = {
            'checkpoint': value,
            'config': json.dumps(config, sort_keys=True),
            'state': self._saved_state_create()
        }

        with open(self.name+'.chkp', 'w') as f:
            f.write(json.dumps(contents))
            f.write('\n')

    def remove_checkpoint(self) -> None:
        try:
            os.remove('{}.chkp'.format(self.name))

        except (IOError, OSError):
            pass

    def _saved_state_restore(self, saved_state: dict) -> None:
        pass

    def _saved_state_create(self) -> dict:
        return {}

    def configure(self) -> None:
        """Applies the config settings stored in `self.config`.

        Called by `__init__`.

        When this method is changed to add/remove new parameters, the class
        docstring should be updated.
        """
        self.infilters = filters.Filters(self.config.get('infilters', []))
        self.outfilters = filters.Filters(self.config.get('outfilters', []))

    def connect(self, inputs: List[str], output: bool) -> None:
        if self.state != ft_states.READY:
            LOG.error('connect called in non ready FT')
            raise AssertionError('connect called in non ready FT')

        for i in inputs:
            LOG.info("%s - requesting fabric sub channel for %s", self.name, i)
            self.chassis.request_sub_channel(
                self.name,
                self,
                i,
                allowed_methods=['update', 'withdraw', 'checkpoint']
            )
        self.inputs = inputs
        self.inputs_checkpoint: Dict[str,str] = {}

        if output:
            self.output = self.chassis.request_pub_channel(self.name)

        self.state = ft_states.CONNECTED

    def apply_infilters(self, origin: Optional[str], method: Optional[str],
                        indicator: Optional[str], value: Optional[dict]) -> Tuple[Optional[str], Optional[dict]]:
        return self.infilters.apply(
            origin=origin,
            method=method,
            indicator=indicator,
            value=value
        )

    def apply_outfilters(self, origin: Optional[str], method: Optional[str],
                        indicator: Optional[str], value: Optional[dict]) -> Tuple[Optional[str], Optional[dict]]:
        return self.outfilters.apply(
            origin=origin,
            method=method,
            indicator=indicator,
            value=value
        )

    @_counting('update.tx')
    def emit_update(self, indicator: str, value: dict) -> None:
        if self.output is None:
            return

        self.trace('EMIT_UPDATE', indicator, value=value)

        indicator, value = self.apply_outfilters(
            origin=self.name,
            method='update',
            indicator=indicator,
            value=value
        )

        if indicator is None:
            return

        if value is not None:
            for k in list(value.keys()):
                if k[0] in ['_', '$']:
                    value.pop(k)

        self.output.publish("update", {
            'source': self.name,
            'indicator': indicator,
            'value': value
        })

    @_counting('withdraw.tx')
    def emit_withdraw(self, indicator: str, value: Optional[dict]=None) -> None:
        if self.output is None:
            return

        self.trace('EMIT_WITHDRAW', indicator, value=value)

        indicator, value = self.apply_outfilters(
            origin=self.name,
            method='withdraw',
            indicator=indicator,
            value=value
        )

        if indicator is None:
            return

        if value is not None:
            for k in list(value.keys()):
                if k[0] in ['_', '$']:
                    value.pop(k)

        self.output.publish("withdraw", {
            'source': self.name,
            'indicator': indicator,
            'value': value
        })

    @_counting('checkpoint.tx')
    def emit_checkpoint(self, value: str) -> None:
        if self.output is None:
            return

        self.output.publish('checkpoint', {
            'source': self.name,
            'value': value
        })

    @_counting('update.rx')
    def update(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[dict]=None) -> None:
        LOG.debug('%s {%s} - update from %s value %s',
                  self.name, self.state, source, value)

        if not self._disable_full_trace:
            self.trace(
                'RECVD_UPDATE',
                indicator if indicator is not None else "-- NONE --",
                source_node=source,
                value=value
            )

        if self.state not in [ft_states.STARTED, ft_states.CHECKPOINT]:
            self.statistics['error.wrong_state'] += 1
            return

        if source in self.inputs_checkpoint:
            LOG.error("update received from checkpointed source")
            raise AssertionError("update received from checkpointed source")

        if value is not None:
            for k in list(value.keys()):
                if k.startswith("_"):
                    value.pop(k)

        fltindicator, fltvalue = self.apply_infilters(
            origin=source,
            method='update',
            indicator=indicator,
            value=value
        )

        if fltindicator is None:
            if not self._disable_full_trace:
                self.trace(
                    'DROP_UPDATE',
                    indicator if indicator is not None else "-- NONE --",
                    source_node=source,
                    value=value
                )

            self.filtered_withdraw(
                source=source,
                indicator=indicator,
                value=value
            )
            return

        self.trace(
            'ACCEPT_UPDATE',
            indicator if indicator is not None else "-- NONE --",
            source_node=source,
            value=value
        )
        self.filtered_update(
            source=source,
            indicator=fltindicator,
            value=fltvalue
        )

    @_counting('update.processed')
    def filtered_update(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[dict]=None) -> None:
        raise NotImplementedError('%s: update' % self.name)

    @_counting('withdraw.rx')
    def withdraw(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[dict]=None) -> None:
        LOG.debug('%s {%s} - withdraw from %s value %s',
                  self.name, self.state, source, value)

        if not self._disable_full_trace:
            self.trace(
                'RECVD_WITHDRAW',
                indicator if indicator is not None else "-- NONE --",
                source_node=source,
                value=value
            )

        if self.state not in [ft_states.STARTED, ft_states.CHECKPOINT]:
            self.statistics['error.wrong_state'] += 1
            return

        if source in self.inputs_checkpoint:
            LOG.error("withdraw received from checkpointed source")
            raise AssertionError("withdraw received from checkpointed source")

        fltindicator, fltvalue = self.apply_infilters(
            origin=source,
            method='withdraw',
            indicator=indicator,
            value=value
        )

        if fltindicator is None:
            if not self._disable_full_trace:
                self.trace(
                    'DROP_WITHDRAW',
                    indicator if indicator is not None else "-- NONE --",
                    source_node=source,
                    value=value
                )
            return

        if fltvalue is not None:
            for k in fltvalue.keys():
                if k.startswith("_"):
                    fltvalue.pop(k)

        self.trace(
            'ACCEPT_WITHDRAW',
            indicator if indicator is not None else "-- NONE --",
            source_node=source,
            value=value
        )
        self.filtered_withdraw(
            source=source,
            indicator=indicator,
            value=value
        )

    @_counting('withdraw.processed')
    def filtered_withdraw(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[dict]=None) -> None:
        raise NotImplementedError('%s: withdraw' % self.name)

    @_counting('checkpoint.rx')
    def checkpoint(self, source: Optional[str]=None, value: Optional[str]=None) -> None:
        LOG.debug('%s {%s} - checkpoint from %s value %s',
                  self.name, self.state, source, value)

        if value is None or source is None:
            raise RuntimeError(f"{self.name} - received an invalid checkpoint: {value} from {source}")

        if self.state not in [ft_states.STARTED, ft_states.CHECKPOINT]:
            LOG.error("%s {%s} - checkpoint received with state not STARTED "
                      "or CHECKPOINT",
                      self.name, self.state)
            raise AssertionError("checkpoint received with state not STARTED "
                                 "or CHECKPOINT")

        for v in self.inputs_checkpoint.values():
            if v != value:
                LOG.error("different checkpoint value received")
                raise AssertionError("different checkpoint value received")

        self.inputs_checkpoint[source] = value

        if len(self.inputs_checkpoint) != len(self.inputs):
            self.state = ft_states.CHECKPOINT
            return

        self.state = ft_states.IDLE
        self.create_checkpoint(value)
        self.last_checkpoint = value
        self.emit_checkpoint(value)

    def _full_trace_timeout(self, timeout: int):
        """To be used as greenlet for disabling full trace after a specific time
        """
        gevent.sleep(timeout)
        self._disable_full_trace = True
        LOG.debug('{} - full trace disabled'.format(self.name))

    def enable_full_trace(self, timeout: int=600):
        """Enables full trace
        """

        # if full trace is already enabled, do nothing
        if self._disable_full_trace is False:
            return

        self._disable_full_trace_glet = gevent.spawn(
            self._full_trace_timeout,
            timeout
        )
        self._disable_full_trace = False
        LOG.debug('{} - full trace enabled'.format(self.name))

    def publish_status(self, force: bool=False):
        if force:
            self._internal_publish_status()

        self._throttled_publish_status()

    def _internal_publish_status(self):
        self._last_status_publish = utils.utc_millisec()
        status = self.mgmtbus_status()
        self.chassis.publish_status(
            timestamp=utils.utc_millisec(),
            nodename=self.name,
            status=status
        )

    # mgmtbus_* methods are called by Chassis
    # when receiving RPC calls
    def mgmtbus_state_info(self) -> dict:
        return {
            'checkpoint': self.last_checkpoint,
            'state': self.state,
            'is_source': len(self.inputs) == 0
        }

    def mgmtbus_initialize(self) -> str:
        self.state = ft_states.INIT
        self.remove_checkpoint()
        self.initialize()
        return 'OK'

    def mgmtbus_rebuild(self) -> str:
        self.state = ft_states.REBUILDING
        self.remove_checkpoint()
        self.rebuild()
        self.state = ft_states.INIT
        return 'OK'

    def mgmtbus_reset(self) -> str:
        self.state = ft_states.RESET
        self.remove_checkpoint()
        self.reset()
        self.state = ft_states.INIT
        return 'OK'

    def mgmtbus_status(self) -> dict:
        try:
            # if node is not ready yet to publish the length
            length: Optional[int] = self.length()
        except:
            length = None

        result = {
            'clock': self._clock,
            'class': (self.__class__.__module__+'.'+self.__class__.__name__),
            'state': self.state,
            'statistics': self.statistics,
            'length': length,
            'inputs': self.inputs,
            'output': (self.output is not None),
            'trace': not self._disable_full_trace
        }
        self._clock += 1
        return result

    def mgmtbus_checkpoint(self, value: Optional[str]=None) -> str:
        if len(self.inputs) != 0:
            # checkpoint on non root nodes is ignored
            return 'ignored'

        if value is None:
            raise RuntimeError(f"{self.name} - null checkpoint")

        self.state = ft_states.IDLE
        self.create_checkpoint(value)
        self.last_checkpoint = value
        self.emit_checkpoint(value)

        return 'OK'

    def mgmtbus_hup(self, source: Optional[str]=None) -> None:
        self.hup(source=source)

    def mgmtbus_signal(self, source: Optional[str]=None, signal: Optional[str]=None, **kwargs) -> Any:
        if signal == 'trace':
            self.enable_full_trace()
            return self._disable_full_trace

        raise NotImplementedError('{}: signal - not implemented'.format(self.name))

    # the following 3 methods are called during
    # initalization. They should be overridden by subclasses and return as fast
    # as possible
    def initialize(self) -> None:
        pass

    def rebuild(self) -> None:
        pass

    def reset(self) -> None:
        pass

    def length(self, source: Optional[str]=None) -> int:
        raise NotImplementedError('%s: length - not implemented' % self.name)

    def hup(self, source: Optional[str]=None) -> None:
        raise NotImplementedError('%s: hup - not implemented' % self.name)

    def trace(self, action: str, indicator: str, **kwargs) -> None:
        if self.state not in [ft_states.STARTED, ft_states.CHECKPOINT]:
            LOG.debug(
                "%s - trace called in wrong state %s",
                self.name,
                self.state
            )
            return

        trace = {
            'indicator': indicator,
            'op': action,
        }
        trace.update(kwargs)
        self.chassis.log(
            timestamp=utils.utc_millisec(),
            nodename=self.name,
            log_type='TRACE',
            value=trace
        )

    def start(self) -> None:
        LOG.debug("%s - start called", self.name)

        if self.state != ft_states.INIT:
            LOG.error("start on not INIT FT")
            raise AssertionError("start on not INIT FT")

        self.state = ft_states.STARTED

    def stop(self) -> None:
        LOG.debug("%s - stop called", self.name)

        if self._disable_full_trace_glet is not None:
            self._disable_full_trace_glet.kill()
            self._disable_full_trace_glet = None

        if self.state not in [ft_states.IDLE, ft_states.STARTED]:
            LOG.error("stop on not IDLE or STARTED FT")
            raise AssertionError("stop on not IDLE or STARTED FT")

        self._throttled_publish_status.cancel()

        self.state = ft_states.STOPPED

    @staticmethod
    def gc(name: str, config: Optional[dict]=None) -> None:
        try:
            os.remove('{}.chkp'.format(name))
        except:
            pass
