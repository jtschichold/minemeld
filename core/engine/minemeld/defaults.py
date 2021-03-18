import multiprocessing

DEFAULT_REDIS_URL = 'unix:///var/run/redis/redis.sock'
DEFAULT_MINEMELD_COMM_PATH = '/var/run/minemeld'
DEFAULT_MINEMELD_STATUS_INTERVAL = '60'
DEFAULT_MINEMELD_MAX_CHASSIS = multiprocessing.cpu_count()
