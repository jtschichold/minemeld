#  Copyright 2015 Palo Alto Networks, Inc
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

import logging
import netaddr
import uuid
import shutil
from typing import (
    List, Union, Any,
    Optional, Dict, Tuple,
    Set, Iterable,
    TYPE_CHECKING
)

from . import base
from . import actorbase
from . import table
from . import st
from .utils import utc_millisec
from .utils import RESERVED_ATTRIBUTES

if TYPE_CHECKING:
    from minemeld.chassis import Chassis

LOG = logging.getLogger(__name__)

WL_LEVEL = st.MAX_LEVEL


class MWUpdate(object):
    def __init__(self, start: int, end: int, uuids: Set[bytes]):
        self.start = start
        self.end = end
        self.uuids: Set[bytes] = set(uuids)

        s = netaddr.IPAddress(start)
        e = netaddr.IPAddress(end)
        self._indicator = '%s-%s' % (s, e)

    def indicator(self):
        return self._indicator

    def __repr__(self):
        return 'MWUpdate('+self._indicator+', %r)' % self.uuids

    def __hash__(self):
        return hash(self._indicator)

    def __eq__(self, other):
        return self.start == other.start and \
            self.end == other.end


class AggregateIPv4FT(actorbase.ActorBaseFT):
    def __init__(self, name: str, chassis: 'Chassis', config: dict) -> None:
        super(AggregateIPv4FT, self).__init__(name, chassis, config)

    def configure(self) -> None:
        super(AggregateIPv4FT, self).configure()

        self.whitelist_prefixes = self.config.get('whitelist_prefixes', [])
        self.enable_list_merge = self.config.get('enable_list_merge', False)

    def _initialize_tables(self, truncate: Optional[bool]=False) -> None:
        self.table: table.Table = table.Table(
            self.name,
            bloom_filter_bits=10,
            truncate=truncate
        )
        self.table.create_index('_id')
        self.st: st.ST = st.ST(self.name+'_st', 32, truncate=truncate)

    def initialize(self) -> None:
        self._initialize_tables()

    def rebuild(self) -> None:
        self._initialize_tables(truncate=True)

    def reset(self) -> None:
        self._initialize_tables(truncate=True)

    def _indicator_key(self, indicator: str, source: str) -> str:
        return indicator+'\x00'+source

    def _calc_indicator_value(self, uuids: Iterable[bytes], additional_uuid: Optional[str]=None, additional_value=None) -> Dict[str,Union[int,str,bool,list]]:
        mv: Dict[str,Union[int,str,bool,list]] = {'sources': []}
        for uuid_ in uuids:
            if uuid_ == additional_uuid:
                v = additional_value
            else:
                # uuid_ = str(uuid.UUID(bytes=uuid_))
                k, v = next(
                    self.table.query('_id', from_key=uuid_.decode('utf-8'), to_key=uuid_.decode('utf-8'),
                                     include_value=True),
                    (None, None)
                )
                if k is None:
                    LOG.error("Unable to find key associated with uuid: %s", uuid_)

            for vk in v:
                if vk in mv and vk in RESERVED_ATTRIBUTES:
                    mv[vk] = RESERVED_ATTRIBUTES[vk](mv[vk], v[vk])
                    continue

                curr_mv = mv.get(vk, None)
                if self.enable_list_merge and isinstance(curr_mv, list) and isinstance(v[vk], list):
                    curr_mv.extend(v[vk])
                    continue

                mv[vk] = v[vk]

        return mv

    def _merge_values(self, origin: str, ov: dict, nv: dict) -> Dict[str,Union[str,int,bool,list]]:
        result: Dict[str,Union[str,int,bool,list]] = {'sources': []}

        result['_added'] = ov['_added']
        result['_id'] = ov['_id']

        for k in nv.keys():
            result[k] = nv[k]

        return result

    def _add_indicator(self, origin: str, indicator: str, value: Dict) -> Tuple[dict,bool]:
        added = False

        now = utc_millisec()
        ik = self._indicator_key(indicator, origin)

        v = self.table.get(ik)
        if v is None:
            v = {
                '_id': str(uuid.uuid4()),
                '_added': now
            }
            added = True
            self.statistics['added'] += 1

        assert isinstance(v, dict)
        v = self._merge_values(origin, v, value)
        v['_updated'] = now

        self.table.put(ik, v)

        return v, added

    def _calc_ipranges(self, start: Optional[int], end: Optional[int]) -> Set[MWUpdate]:
        """Calc IP Ranges overlapping the range between start and end
        
        Args:
            start (int): start of the range
            end (int): end of the range
        
        Returns:
            set: set of ranges
        """

        result: Set[MWUpdate] = set()

        # collect the endpoint between start and end
        eps_set: Set[int] = set()
        for epaddr, _, _, _ in self.st.query_endpoints(start=start, stop=end):
            eps_set.add(epaddr)
        eps = sorted(eps_set)

        if len(eps) == 0:
            return result

        # walk thru the endpoints, tracking last endpoint
        # current level, active segments and segments levels
        oep = None
        oeplevel = -1
        live_ids: Set[bytes] = set()
        slevels = {}

        for epaddr in eps:
            # for each endpoint we track which segments are starting
            # and which ones are ending with that specific endpoint
            end_ids = set()
            start_ids = set()
            eplevel = 0
            for cuuid, clevel, cstart, cend in self.st.cover(epaddr):
                slevels[cuuid] = clevel

                if clevel > eplevel:
                    eplevel = clevel
                if cstart == epaddr:
                    start_ids.add(cuuid)
                if cend == epaddr:
                    end_ids.add(cuuid)

                if cend != epaddr and cstart != epaddr:
                    if cuuid not in live_ids:
                        assert epaddr == eps[0]
                        live_ids.add(cuuid)

            assert len(end_ids) + len(start_ids) > 0

            if len(start_ids) != 0:
                if oep is not None and oep != epaddr and len(live_ids) != 0:
                    if oeplevel != WL_LEVEL:
                        result.add(MWUpdate(oep, epaddr-1,
                                            live_ids))

                oep = epaddr
                oeplevel = eplevel
                live_ids = live_ids | start_ids

            if len(end_ids) != 0:
                if oep is not None and len(live_ids) != 0:
                    if eplevel < WL_LEVEL:
                        result.add(MWUpdate(oep, epaddr, live_ids))

                oep = epaddr+1
                live_ids = live_ids - end_ids

                oeplevel = eplevel
                if len(live_ids) != 0:
                    oeplevel = max([slevels[id_] for id_ in live_ids])

        return result

    def _range_from_indicator(self, indicator: str) -> Tuple[Optional[int],Optional[int]]:
        if '-' in indicator:
            start, end = [int(netaddr.IPAddress(x)) for x in indicator.split('-', 1)]
        elif '/' in indicator:
            ipnet = netaddr.IPNetwork(indicator)
            start = int(ipnet.ip)
            end = start+ipnet.size-1
        else:
            start = int(netaddr.IPAddress(indicator))
            end = start

        if (not (start >= 0 and start <= 0xFFFFFFFF)) or \
           (not (end >= 0 and end <= 0xFFFFFFFF)):
            LOG.error('%s - {%s} invalid IPv4 indicator',
                      self.name, indicator)
            return None, None

        return start, end

    def _endpoints_from_range(self, start: int, end: int) -> Tuple[Optional[int],Optional[int]]:
        """Return last endpoint before range and first endpoint after range
        
        Args:
            start (int): range start
            end (int): range stop
        
        Returns:
            tuple: (last endpoint before, first endpoint after)
        """

        rangestart: Optional[int] = None
        ep_start: Optional[Tuple[int,int,bool,bytes]] = next(
            self.st.query_endpoints(start=0, stop=max(start-1, 0),
                                    reverse=True),
            None
        )
        if ep_start is not None:
            rangestart = ep_start[0]
        LOG.debug('%s - range start: %s', self.name, rangestart)

        rangestop: Optional[int] = None
        ep_stop: Optional[Tuple[int,int,bool,bytes]] = next(
            self.st.query_endpoints(reverse=False,
                                    start=min(end+1, self.st.max_endpoint),
                                    stop=self.st.max_endpoint,
                                    include_start=False),
            None
        )
        if ep_stop is not None:
            rangestop = ep_stop[0]
        LOG.debug('%s - range stop: %s', self.name, rangestop)

        return rangestart, rangestop

    @base._counting('update.processed')
    def filtered_update(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[Dict[str,Union[str,int,bool]]]=None) -> None:
        if value is None or source is None or indicator is None:
            return

        vtype = value.get('type', None)
        if vtype != 'IPv4':
            self.statistics['update.ignored'] += 1
            return

        v, newindicator = self._add_indicator(source, indicator, value)

        start, end = self._range_from_indicator(indicator)
        if start is None or end is None:
            return

        level = 1
        for p in self.whitelist_prefixes:
            if source.startswith(p):
                level = WL_LEVEL
                break

        LOG.debug("%s - update: indicator: (%s) %s %s level: %s",
                  self.name, indicator, start, end, level)

        rangestart, rangestop = self._endpoints_from_range(start, end)

        rangesb: Set[MWUpdate] = set(self._calc_ipranges(rangestart, rangestop))
        LOG.debug('%s - ranges before update: %s', self.name, rangesb)

        if not newindicator and level != WL_LEVEL:
            for u in rangesb:
                self.emit_update(
                    u.indicator(),
                    self._calc_indicator_value(u.uuids)
                )
            return

        uuidbytes = v['_id'].encode('utf-8')
        self.st.put(uuidbytes, start, end, level=level)

        rangesa: Set[MWUpdate] = set(self._calc_ipranges(rangestart, rangestop))
        LOG.debug('%s - ranges after update: %s', self.name, rangesa)

        added: Set[MWUpdate] = rangesa-rangesb
        LOG.debug("%s - IP ranges added: %s", self.name, added)

        removed = rangesb-rangesa
        LOG.debug("%s - IP ranges removed: %s", self.name, removed)

        for u in added:
            self.emit_update(
                u.indicator(),
                self._calc_indicator_value(u.uuids)
            )

        for u in rangesa - added:
            for ou in rangesb:
                if u == ou and len(u.uuids ^ ou.uuids) != 0:
                    LOG.debug("IP range updated: %s", repr(u))
                    self.emit_update(
                        u.indicator(),
                        self._calc_indicator_value(u.uuids)
                    )

        for u in removed:
            self.emit_withdraw(
                u.indicator(),
                value=self._calc_indicator_value(u.uuids)
            )

    @base._counting('withdraw.processed')
    def filtered_withdraw(self, source: Optional[str]=None, indicator: Optional[str]=None, value: Optional[dict]=None):
        LOG.debug("%s - withdraw from %s - %s", self.name, source, indicator)

        if source is None or indicator is None:
            return

        if value is not None and value.get('type', None) != 'IPv4':
            self.statistics['withdraw.ignored'] += 1
            return

        ik = self._indicator_key(indicator, source)

        v = self.table.get(ik)
        LOG.debug("%s - v: %s", self.name, v)
        if v is None:
            return
        assert isinstance(v, dict)

        self.table.delete(ik)
        self.statistics['removed'] += 1

        start, end = self._range_from_indicator(indicator)
        if start is None or end is None:
            return

        level = 1
        for p in self.whitelist_prefixes:
            if source.startswith(p):
                level = WL_LEVEL
                break

        rangestart, rangestop = self._endpoints_from_range(start, end)

        rangesb = set(self._calc_ipranges(rangestart, rangestop))
        LOG.debug("ranges before: %s", rangesb)

        uuidbytes = v['_id'].encode('utf-8')
        self.st.delete(uuidbytes, start, end, level=level)

        rangesa = set(self._calc_ipranges(rangestart, rangestop))
        LOG.debug("ranges after: %s", rangesa)

        added = rangesa-rangesb
        LOG.debug("IP ranges added: %s", added)

        removed = rangesb-rangesa
        LOG.debug("IP ranges removed: %s", removed)

        for u in added:
            self.emit_update(
                u.indicator(),
                self._calc_indicator_value(u.uuids)
            )

        for u in rangesa - added:
            for ou in rangesb:
                if u == ou and len(u.uuids ^ ou.uuids) != 0:
                    LOG.debug("IP range updated: %s", repr(u))
                    self.emit_update(
                        u.indicator(),
                        self._calc_indicator_value(u.uuids)
                    )

        for u in removed:
            self.emit_withdraw(
                u.indicator(),
                value=self._calc_indicator_value(
                    u.uuids,
                    additional_uuid=v['_id'].encode('utf-8'),
                    additional_value=v
                )
            )

    def length(self, source: Optional[str]=None) -> int:
        return self.table.num_indicators

    def stop(self) -> None:
        super(AggregateIPv4FT, self).stop()

        self.table.close()

        LOG.info("%s - # indicators: %d", self.name, self.table.num_indicators)

    @staticmethod
    def gc(name, config=None) -> None:
        actorbase.ActorBaseFT.gc(name, config=config)

        shutil.rmtree(name, ignore_errors=True)
        shutil.rmtree('{}_st'.format(name), ignore_errors=True)
