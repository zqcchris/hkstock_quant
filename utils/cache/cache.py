""" 基于redis的缓存设计 """
import os
import sys
sys.path.append(os.getcwd())
import pickle
import redis
myredis = redis.Redis(host='localhost', port=6379, decode_responses=False)
expired = 3 * 24 * 3600  # 设置过期的秒数, 默认为3天

"""
Ubuntu 按照redis：sudo apt-get install redis-server
"""


def cache_set(cache, key, value):
    if isinstance(cache, redis.Redis):
        cache.set(key, value, ex=expired)
    elif isinstance(cache, dict):
        cache[key] = value
    else:
        raise ValueError("Cache错误")


def cache_set_object(cache, key, value):
    if isinstance(cache, redis.Redis):
        cache.set(key, pickle.dumps(value), ex=expired)
    elif isinstance(cache, dict):
        cache[key] = value
    else:
        raise ValueError("Cache对象错误")


def cache_get(cache, key):
    if isinstance(cache, redis.Redis):
        value = cache.get(key)
        if value is None: value = ""
        if isinstance(value, bytes):
            value = value.decode("utf8")
        return value
    elif isinstance(cache, dict):
        return cache.get(key, "")
    else:
        raise ValueError("Cache错误")


def cache_get_object(cache, key):
    if isinstance(cache, redis.Redis):
        value = cache.get(key)
        if value is None: return None
        return pickle.loads(value)
    elif isinstance(cache, dict):
        return cache.get(key, "")
    else:
        raise ValueError("Cache对象错误")


def cache_delete(cache, key):
    if isinstance(cache, redis.Redis):
        cache.delete(key)
    elif isinstance(cache, dict):
        if key in cache.keys():
            del cache[key]
    else:
        raise ValueError("Cache对象删除错误")
