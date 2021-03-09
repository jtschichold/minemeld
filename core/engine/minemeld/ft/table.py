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

"""
Table implementation based on LevelDB (https://github.com/google/leveldb).
This is a sort of poor, lazy man implementation of IndexedDB schema.

**KEYS**

Numbers are 8-bit unsigned.

- Schema Version: (0)
- Index Last Global Id: (0,1, <indexnum>)
- Last Update Key: (0,2)
- Number of Indicators: (0,3)
- Table Last Global ID: (0,4)
- Custom Metadata: (0,5)
- Indicator Version: (1,0,<indicator>)
- Indicator: (1,1,<indicator>)

**INDICATORS**

Each indicators has 2 entries associated in the DB: a version and a value.

The version number is used to track indicator existance and versioning.
When an indicator value is updated, its version number is incremented.
The version number is a 64-bit LSB unsigned int.

The value of an indicator is a 64-bit unsigned int LSB followed by a dump of
a dictionary of attributes in JSON format.

To iterate over all the indicators versions iterate from key (1,0) to key
(1,1) excluded.

NULL indicators are not allowed.

**INDEXES**

Indicators are stored in alphabetical order. Indexes are secondary indexes
on indicators attributes.

Each index has an associated id in the range 0 - 255. The attribute associated
to the index is stored at (0,1,<index id>), if the key does not exist the
index does not exist.

There is also a Last Global Id per index, used to index indicators with the
same attribute value. Each time a new indicator is added to the index, the
Last Global Id is incremented. The Last Global Id of an index is stored at
(2,<index id>,0) as a 64-bit LSB unsigned int.

Each entry in the index is stored with a key
(2,<index id>,0xF0,<encoded value>,<last global id>) and value
(<version>,<indicator>). <encoded value> depends on the type of attribute.

When iterating over an index, the value of an index entry is loaded and if
the version does not match with current indicator version the index entry is
deleted. This permits a sort of lazy garbage collection.

To retrieve all the indicators with a specific attribute value just iterate
over the keys (2,<index id>,0xF0,<encoded value>) and
(2,<index id>,0xF0,<encoded value>,0xFF..FF)
"""

import os
import plyvel
import struct
import ujson
import time
import logging
import shutil
import gevent
from typing import (
    Optional, Union, Iterator,
    Tuple, Protocol, ContextManager,
    Dict,
    TYPE_CHECKING, TypeVar,
)


SCHEMAVERSION_KEY = struct.pack("B", 0)
START_INDEX_KEY = struct.pack("BBB", 0, 1, 0)
END_INDEX_KEY = struct.pack("BBB", 0, 1, 0xFF)
LAST_UPDATE_KEY = struct.pack("BB", 0, 2)
NUM_INDICATORS_KEY = struct.pack("BB", 0, 3)
TABLE_LAST_GLOBAL_ID = struct.pack("BB", 0, 4)
CUSTOM_METADATA = struct.pack("BB", 0, 5)

LOG = logging.getLogger(__name__)


if TYPE_CHECKING:
    class DbIteratorBB(Iterator[Tuple[bytes,bytes]],ContextManager[None]): # pylint: disable=duplicate-bases
        pass

    class DbIteratorB(Iterator[bytes],ContextManager[None]): # pylint: disable=duplicate-bases
        pass


class InvalidTableException(Exception):
    pass


class Table(object):
    def __init__(self, name, truncate=False, bloom_filter_bits=0):
        if truncate:
            try:
                shutil.rmtree(name)
            except:
                pass

        self.db = None
        self._compact_glet = None

        self.db = plyvel.DB(
            name,
            create_if_missing=True,
            bloom_filter_bits=bloom_filter_bits
        )
        self._read_metadata()

        self.compact_interval = int(os.environ.get('MM_TABLE_COMPACT_INTERVAL', 3600 * 6))
        self.compact_delay = int(os.environ.get('MM_TABLE_COMPACT_DELAY', 3600))
        self._compact_glet = gevent.spawn(self._compact_loop)

    def _init_db(self) -> None:
        self.last_update = 0
        self.indexes: Dict[str,dict] = {}
        self.num_indicators = 0
        self.last_global_id = 0

        batch = self.db.write_batch()
        batch.put(SCHEMAVERSION_KEY, struct.pack("B", 1))
        batch.put(LAST_UPDATE_KEY, struct.pack(">Q", self.last_update))
        batch.put(NUM_INDICATORS_KEY, struct.pack(">Q", self.num_indicators))
        batch.put(TABLE_LAST_GLOBAL_ID, struct.pack(">Q", self.last_global_id))
        batch.write()

    def _read_metadata(self) -> None:
        sv = self._get(SCHEMAVERSION_KEY)
        if sv is None:
            return self._init_db()
        sv = struct.unpack("B", sv)[0]
        if sv == 0:
            # add table last global id
            self._upgrade_from_s0()
        elif sv == 1:
            pass
        else:
            raise InvalidTableException("Schema version not supported")

        self.indexes = {}
        ri: 'DbIteratorBB' = self.db.iterator(
            start=START_INDEX_KEY,
            stop=END_INDEX_KEY
        )
        with ri:
            for k, v in ri:
                _, _, indexid = struct.unpack("BBB", k)
                decoded_v = v.decode('utf-8')
                if decoded_v in self.indexes:
                    raise InvalidTableException("2 indexes with the same name")
                self.indexes[decoded_v] = {
                    'id': indexid,
                    'last_global_id': 0
                }
        for i in self.indexes:
            lgi = self._get(self._last_global_id_key(self.indexes[i]['id']))
            if lgi is not None:
                self.indexes[i]['last_global_id'] = struct.unpack(">Q", lgi)[0]
            else:
                self.indexes[i]['last_global_id'] = -1

        t = self._get(LAST_UPDATE_KEY)
        if t is None:
            raise InvalidTableException("LAST_UPDATE_KEY not found")
        self.last_update = struct.unpack(">Q", t)[0]

        t = self._get(NUM_INDICATORS_KEY)
        if t is None:
            raise InvalidTableException("NUM_INDICATORS_KEY not found")
        self.num_indicators = struct.unpack(">Q", t)[0]

        t = self._get(TABLE_LAST_GLOBAL_ID)
        if t is None:
            raise InvalidTableException("TABLE_LAST_GLOBAL_ID not found")
        self.last_global_id = struct.unpack(">Q", t)[0]

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            result = self.db.get(key)
        except KeyError:
            return None

        return result

    def __del__(self) -> None:
        self.close()

    def get_custom_metadata(self) -> Optional[dict]:
        cmetadata = self._get(CUSTOM_METADATA)
        if cmetadata is None:
            return None
        return ujson.loads(cmetadata)

    def set_custom_metadata(self, metadata: Optional[dict] =None) -> None:
        if metadata is None:
            self.db.delete(CUSTOM_METADATA)
            return

        cmetadata = ujson.dumps(metadata)
        self.db.put(CUSTOM_METADATA, cmetadata.encode('utf-8'))

    def close(self) -> None:
        if self.db is not None:
            self.db.close()

        if self._compact_glet is not None:
            self._compact_glet.kill()

        self.db = None
        self._compact_glet = None

    def exists(self, key: str) -> bool:
        ikeyv = self._indicator_key_version(key.encode('utf-8'))
        return (self._get(ikeyv) is not None)

    def get(self, key: str) -> Union[None, dict, str]:
        ikey = self._indicator_key(key.encode('utf-8'))
        value = self._get(ikey)
        if value is None:
            return None

        # skip version
        return ujson.loads(value[8:])

    def delete(self, key: str) -> None:
        bkey = key.encode('utf8')
        ikey = self._indicator_key(bkey)
        ikeyv = self._indicator_key_version(bkey)

        if self._get(ikeyv) is None:
            return

        batch = self.db.write_batch()
        batch.delete(ikey)
        batch.delete(ikeyv)
        self.num_indicators -= 1
        batch.put(NUM_INDICATORS_KEY, struct.pack(">Q", self.num_indicators))
        batch.write()

    def _indicator_key(self, key: bytes) -> bytes:
        return struct.pack("BB", 1, 1) + key

    def _indicator_key_version(self, key: bytes) -> bytes:
        return struct.pack("BB", 1, 0) + key

    def _index_key(self, idxid: int, value: Union[str, int], lastidxid: Optional[int]=None) -> bytes:
        key = struct.pack("BBB", 2, idxid, 0xF0)

        if isinstance(value, str):
            bvalue = value.encode('utf-8')
            key += struct.pack(">BL", 0x0, len(bvalue))+bvalue
        elif isinstance(value, int):
            key += struct.pack(">BQ", 0x1, value)
        else:
            raise ValueError("Unhandled value type: %s" % type(value))

        if lastidxid is not None:
            key += struct.pack(">Q", lastidxid)

        return key

    def _last_global_id_key(self, idxid: int) -> bytes:
        return struct.pack("BBB", 2, idxid, 0)

    def create_index(self, attribute: str) -> None:
        if attribute in self.indexes:
            return

        if len(self.indexes) == 0:
            idxid = 0
        else:
            idxid = max([i['id'] for i in self.indexes.values()])+1

        self.indexes[attribute] = {
            'id': idxid,
            'last_global_id': -1
        }

        batch = self.db.write_batch()
        batch.put(struct.pack("BBB", 0, 1, idxid), attribute.encode('utf-8'))
        batch.write()

    def put(self, key: str, value: dict) -> None:
        if type(value) != dict:
            raise ValueError()

        bkey = key.encode('utf-8')
        ikey = self._indicator_key(bkey)
        ikeyv = self._indicator_key_version(bkey)

        exists = self._get(ikeyv)
        self.last_global_id += 1
        cversion = self.last_global_id

        now = time.time()
        self.last_update = int(now)

        batch = self.db.write_batch()
        batch.put(ikey, struct.pack(">Q", cversion)+ujson.dumps(value).encode('utf-8'))
        batch.put(ikeyv, struct.pack(">Q", cversion))
        batch.put(LAST_UPDATE_KEY, struct.pack(">Q", int(self.last_update)))
        batch.put(TABLE_LAST_GLOBAL_ID, struct.pack(">Q", self.last_global_id))

        if exists is None:
            self.num_indicators += 1
            batch.put(
                NUM_INDICATORS_KEY,
                struct.pack(">Q", self.num_indicators)
            )

        for iattr, index in self.indexes.items():
            v = value.get(iattr, None)
            if v is None:
                continue

            index['last_global_id'] += 1

            idxkey = self._index_key(index['id'], v, index['last_global_id'])
            batch.put(idxkey, struct.pack(">Q", cversion) + bkey)

            batch.put(
                self._last_global_id_key(index['id']),
                struct.pack(">Q", index['last_global_id'])
            )

        batch.write()

    def query(self, index=None, from_key: Union[str,int,None]=None, to_key: Union[str,int,None]=None,
              include_value: bool =False, include_stop: bool =True, include_start: bool =True,
              reverse: bool =False) -> Iterator[Tuple[str, Optional[Union[None, dict, str]]]]:
        if index is None:
            if isinstance(from_key, int):
                raise TypeError("from_key cannot be int if index is None")
            if isinstance(to_key, int):
                raise TypeError("to_key cannot be int if index is None")
            return self._query_by_indicator(
                from_key=from_key if from_key is None else from_key.encode('utf-8'),
                to_key=to_key if to_key is None else to_key.encode('utf-8'),
                include_value=include_value,
                include_stop=include_stop,
                include_start=include_start,
                reverse=reverse
            )
        return self._query_by_index(
            index,
            from_key=from_key,
            to_key=to_key,
            include_value=include_value,
            include_stop=include_stop,
            include_start=include_start,
            reverse=reverse
        )

    def _query_by_indicator(self, from_key: Optional[bytes]=None, to_key: Optional[bytes]=None,
                            include_value: bool=False, include_stop: bool=True,
                            include_start: bool=True, reverse: bool=False) -> Iterator[Tuple[str, Optional[Union[None, dict, str]]]]:
        if from_key is None:
            from_key = struct.pack("BB", 1, 1)
            include_stop = False
        else:
            from_key = self._indicator_key(from_key)

        if to_key is None:
            to_key = struct.pack("BB", 1, 2)
            include_start = False
        else:
            to_key = self._indicator_key(to_key)

        ri: 'DbIteratorB' = self.db.iterator(
            start=from_key,
            stop=to_key,
            include_stop=include_stop,
            include_start=include_start,
            reverse=reverse,
            include_value=False
        )
        with ri:
            for ekey in ri:
                ekey = ekey[2:]
                decoded_ekey = ekey.decode('utf8', 'ignore')
                if include_value:
                    yield decoded_ekey, self.get(decoded_ekey)
                else:
                    yield decoded_ekey, None

    def _query_by_index(self, index: str, from_key: Union[str,int,None]=None, to_key: Union[str,int,None]=None,
                            include_value: bool=False, include_stop: bool=True,
                            include_start: bool=True, reverse: bool=False) -> Iterator[Tuple[str, Optional[Union[None, dict, str]]]]:
        if index not in self.indexes:
            raise ValueError()

        idxid = self.indexes[index]['id']

        if from_key is None:
            bfrom_key = struct.pack("BBB", 2, idxid, 0xF0)
            include_start = False
        else:
            bfrom_key = self._index_key(idxid, from_key)

        if to_key is None:
            bto_key = struct.pack("BBB", 2, idxid, 0xF1)
            include_stop = False
        else:
            bto_key = self._index_key(
                idxid,
                to_key,
                lastidxid=0xFFFFFFFFFFFFFFFF
            )

        ldeleted = 0
        ri: 'DbIteratorBB' = self.db.iterator(
            start=bfrom_key,
            stop=bto_key,
            include_value=True,
            include_start=include_start,
            include_stop=include_stop,
            reverse=reverse
        )
        with ri:
            for ikey, ekey in ri:
                iversion = struct.unpack(">Q", ekey[:8])[0]
                ekey = ekey[8:]

                evalue = self._get(self._indicator_key_version(ekey))
                if evalue is None:
                    # LOG.debug("Key does not exist")
                    # key does not exist
                    self.db.delete(ikey)
                    ldeleted += 1
                    continue

                cversion = struct.unpack(">Q", evalue)[0]
                if iversion != cversion:
                    # index value is old
                    # LOG.debug("Version mismatch")
                    self.db.delete(ikey)
                    ldeleted += 1
                    continue

                decoded_ekey = ekey.decode('utf-8', 'ignore')
                if include_value:
                    yield decoded_ekey, self.get(decoded_ekey)
                else:
                    yield decoded_ekey, None

        LOG.info('Deleted in scan of {}: {}'.format(index, ldeleted))

    def _compact_loop(self) -> None:
        gevent.sleep(self.compact_delay)

        while True:
            try:
                gevent.idle()

                counter = 0
                for idx in self.indexes.keys():
                    for _ in self.query(index=idx, include_value=False):
                        if counter % 512 == 0:
                            gevent.sleep(0.001)  # yield to other greenlets
                        counter += 1

            except gevent.GreenletExit:
                break
            except:
                LOG.exception('Exception in _compact_loop')

            try:
                gevent.sleep(self.compact_interval)

            except gevent.GreenletExit:
                break

    def _upgrade_from_s0(self) -> None:
        LOG.info('Upgrading from schema version 0 to schema version 1')

        LOG.info('Loading indexes...')
        indexes: Dict[str, dict] = {}
        ri: 'DbIteratorBB' = self.db.iterator(
            start=START_INDEX_KEY,
            stop=END_INDEX_KEY
        )
        with ri:
            for k, v in ri:
                _, _, indexid = struct.unpack("BBB", k)
                decoded_v = v.decode('utf-8')
                if decoded_v in indexes:
                    raise InvalidTableException("2 indexes with the same name")
                indexes[decoded_v] = {
                    'id': indexid,
                    'last_global_id': 0
                }
        for i in indexes:
            lgi = self._get(self._last_global_id_key(indexes[i]['id']))
            if lgi is not None:
                indexes[i]['last_global_id'] = struct.unpack(">Q", lgi)[0]
            else:
                indexes[i]['last_global_id'] = -1

        LOG.info('Scanning indexes...')
        last_global_id = 0
        for i, idata in indexes.items():
            from_key = struct.pack("BBB", 2, idata['id'], 0xF0)
            include_start = False
            to_key = struct.pack("BBB", 2, idata['id'], 0xF1)
            include_stop = False

            ri = self.db.iterator(
                start=from_key,
                stop=to_key,
                include_value=True,
                include_start=include_start,
                include_stop=include_stop,
                reverse=False
            )
            with ri:
                for _, ekey in ri:
                    iversion = struct.unpack(">Q", ekey[:8])[0]
                    if iversion > last_global_id:
                        last_global_id = iversion+1

        LOG.info('Last global id: {}'.format(last_global_id))
        batch = self.db.write_batch()
        batch.put(SCHEMAVERSION_KEY, struct.pack("B", 1))
        batch.put(TABLE_LAST_GLOBAL_ID, struct.pack(">Q", last_global_id))
        batch.write()
