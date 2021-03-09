import os
from typing import cast

import redis
import werkzeug.local

from flask import g

from . import REDIS_URL
from .logger import LOG


__all__ = ['init_app', 'SR']


REDIS_CP = redis.ConnectionPool.from_url(
    REDIS_URL,
    max_connections=int(os.environ.get('REDIS_MAX_CONNECTIONS', 200)),
    decode_responses=True,
    encoding="utf-8"
)


REDIS_CP_RAW = redis.ConnectionPool.from_url(
    REDIS_URL,
    max_connections=int(os.environ.get('REDIS_MAX_CONNECTIONS', 10)),
    decode_responses=False,
)


def get_SR():
    SR = getattr(g, '_redis_client', None)
    if SR is None:
        SR = redis.StrictRedis(connection_pool=REDIS_CP)
        g._redis_client = SR
    return SR


def get_SR_raw():
    result = getattr(g, '_redis_client_raw', None)
    if result is None:
        result = redis.StrictRedis(connection_pool=REDIS_CP_RAW)
        g._redis_client = result
    return result


def teardown(exception):
    SR = getattr(g, '_redis_client', None)
    if SR is not None:
        g._redis_client = None
        LOG.debug(
            'redis connection pool: in use: {} available: {}'.format(
                len(REDIS_CP._in_use_connections),
                len(REDIS_CP._available_connections)
            )
        )

    SR_raw = getattr(g, '_redis_client_raw', None)
    if SR_raw is not None:
        g._redis_client_raw = None
        LOG.debug(
            'redis connection pool: in use: {} available: {}'.format(
                len(REDIS_CP_RAW._in_use_connections),
                len(REDIS_CP_RAW._available_connections)
            )
        )


SR = cast(redis.StrictRedis, werkzeug.local.LocalProxy(get_SR))
SR_RAW = cast(redis.StrictRedis, werkzeug.local.LocalProxy(get_SR_raw))


def init_app(app):
    app.teardown_appcontext(teardown)
