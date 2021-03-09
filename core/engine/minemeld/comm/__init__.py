from .zmqredis import ZMQRedis


def factory(commclass: str, config: dict) -> ZMQRedis:
    if commclass == 'ZMQRedis':
        return ZMQRedis(config)

    return ZMQRedis(config)


def cleanup(commclass: str, config: dict):
    return ZMQRedis.cleanup(config)
