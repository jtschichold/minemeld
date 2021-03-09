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

import os
import os.path
import hashlib
import struct
from time import time
from typing import Set

from .redisclient import SR, SR_RAW

from flask import request, jsonify

from . import config
from .aaa import MMBlueprint
from .logger import LOG


__all__ = ['BLUEPRINT']


BLUEPRINT = MMBlueprint('metrics', __name__, url_prefix='/metrics')
DT_TO_PMCOLLECTD = {
    3600: 'day',
    86400: 'day',
    24 * 3600 * 7: 'week',
    30 * 3600 * 24: 'month'
}


def _list_metrics(prefix=None):
    metrics: Set[str] = set()
    for key in SR.keys('pmcollectd:*' if prefix is None else f'pmcollectd:{prefix}*'):
        _, metric_name = key.split(':', 1)
        metric_name, _ = metric_name.split('\x00')
        metrics.add(metric_name)

    return list(metrics)


def _fetch_metric(metric, dt=86400):
    threshold = int(time()) - dt
    result = []

    cdp = DT_TO_PMCOLLECTD[dt]
    ctimestamp_hi_raw, cbin_raw, cvalue_raw, cdps_raw = SR_RAW.hmget(f'pmcollectd:{metric}\x00cdp:{cdp}', 'ctimestamp_hi', 'cbin', 'cvalue', 'cdps')

    if ctimestamp_hi_raw is None:
        return result
    ctimestamp_hi = int(ctimestamp_hi_raw.decode('utf-8')) << 32

    if cbin_raw is None or cvalue_raw is None:
        return result
    cbin = int(cbin_raw.decode('utf-8'))
    cvalue = int(cvalue_raw.decode('utf-8'))

    if cdps_raw is not None:
        for i in range(0, len(cdps_raw), 16):
            binc, valuec = struct.unpack('Qq', cdps_raw[i:i+16])
            if ctimestamp_hi + binc < threshold:
                continue

            result.append([ctimestamp_hi + binc, valuec])
    
    if ctimestamp_hi + cbin >= threshold:
        result.append([ctimestamp_hi + cbin, cvalue])

    return result


@BLUEPRINT.route('/', read_write=False)
def get_metrics():
    return jsonify(result=_list_metrics())


@BLUEPRINT.route('/minemeld/<nodetype>', read_write=False)
def get_node_type_metrics(nodetype):
    try:
        dt = int(request.args.get('dt', '86400'))
    except ValueError:
        return jsonify(error={'message': 'Invalid delta'}), 400
    if dt not in DT_TO_PMCOLLECTD:
        return jsonify(error={'message': 'Invalid delta'}), 400

    metrics = _list_metrics(prefix=f'minemeld:{nodetype}.')

    result = []
    for m in metrics:
        LOG.debug(f'fetching {m[9:]}')
        v = _fetch_metric(m, dt=dt)

        _, mname = m[9:].split('.', 1)

        result.append({
            'metric': mname,
            'values': v
        })

    return jsonify(result=result)


@BLUEPRINT.route('/minemeld', read_write=False)
def get_global_metrics():
    try:
        dt = int(request.args.get('dt', '86400'))
    except ValueError:
        return jsonify(error={'message': 'Invalid delta'}), 400
    if dt not in DT_TO_PMCOLLECTD:
        return jsonify(error={'message': 'Invalid delta'}), 400

    metrics = _list_metrics(prefix='minemeld:')
    metrics = [m for m in metrics if 'minemeld.sources' not in m]
    metrics = [m for m in metrics if 'minemeld.outputs' not in m]
    metrics = [m for m in metrics if 'minemeld.transits' not in m]

    result = []
    for m in metrics:
        v = _fetch_metric(m, dt=dt)

        result.append({
            'metric': m[9:],
            'values': v
        })

    return jsonify(result=result)


@BLUEPRINT.route('/<node>', read_write=False)
def get_node_metrics(node):
    try:
        dt = int(request.args.get('dt', '86400'))
    except ValueError:
        return jsonify(error={'message': 'Invalid delta'}), 400
    if dt not in DT_TO_PMCOLLECTD:
        return jsonify(error={'message': 'Invalid delta'}), 400

    metrics = _list_metrics(prefix=f'node:{node}.')

    result = []
    for m in metrics:
        v = _fetch_metric(m, dt=dt)

        _, mname = m[5:].split('.', 1)

        result.append({
            'metric': mname,
            'values': v
        })

    return jsonify(result=result)


@BLUEPRINT.route('/<node>/<metric>', methods=['GET'], read_write=False)
def get_metric(node, metric):
    try:
        dt = int(request.args.get('dt', '86400'))
    except ValueError:
        return jsonify(error={'message': 'Invalid delta'}), 400
    if dt not in DT_TO_PMCOLLECTD:
        return jsonify(error={'message': 'Invalid delta'}), 400

    metric = f'node:{node}.{metric}'

    if metric not in _list_metrics(prefix=metric):
        return jsonify(error={'message': 'Unknown metric'}), 404

    try:
        result = _fetch_metric(metric, dt=dt)
    except RuntimeError as e:
        return jsonify(error={'message': str(e)}), 400

    return jsonify(result=result)
