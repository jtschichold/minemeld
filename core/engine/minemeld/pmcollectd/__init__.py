#  Copyright 2015-2020 Palo Alto Networks, Inc
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
minemeld.pmcollectd

Provides a client to Redis for storing metrics.
"""

import logging
from hashlib import sha1
from time import time
from typing import (
    Optional, List, Tuple,
    TYPE_CHECKING
)

from .update import UPDATE

if TYPE_CHECKING:
    import redis

LOG = logging.getLogger(__name__)
UPDATE_SHA1 = sha1(UPDATE).hexdigest()
MAX_VALUE = 2**31


class PMCollectdClient(object):
    """Collectd client.

    Args:
        redis (redis.Redis): Redis instance
    """
    def __init__(self, SR: 'redis.Redis') -> None:
        self.SR = SR
        self.inited: bool = False
        self.update_sha1: str = UPDATE_SHA1

    def init(self) -> None:
        if self.inited:
            return

        scripts_exist: List[bool] = self.SR.script_exists(
            UPDATE_SHA1
        )
        if not scripts_exist[0]:
            self.update_sha1 = self.SR.script_load(UPDATE)
            if self.update_sha1 != UPDATE_SHA1:
                LOG.error(f"Redis SHA1 for update script differs from locally calculated: {self.update_sha1} vs {UPDATE_SHA1}")

    def put(self, metrics: List[Tuple[str,int]], timestamp: Optional[int]=None) -> None:
        self.init()

        if timestamp is None:
            timestamp = int(time())

        timestamp_lo = timestamp & 0xFFFFFFFF
        timestamp_hi = timestamp >> 32

        with self.SR.pipeline(transaction=False) as pipe:
            for m in metrics:
                identifier, value = m
                LOG.info(f"metric: {identifier}: {value}")

                if value > MAX_VALUE or value < -MAX_VALUE:
                    LOG.error(f"Metric value too large for {identifier}: {value}")
                    continue

                pipe.evalsha(self.update_sha1, 0,
                    identifier,
                    timestamp_hi, timestamp_lo,
                    value
                )

            pipe.execute()
