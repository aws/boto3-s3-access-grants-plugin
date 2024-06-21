from cacheout import Cache

ACCESS_DENIED_CACHE_SIZE = 3000
ACCESS_DENIED_CACHE_TTL = 5 * 60   # 5 mins


class AccessDeniedCache:
    access_denied_cache = None

    def __init__(self, cache_size=ACCESS_DENIED_CACHE_SIZE, ttl=ACCESS_DENIED_CACHE_TTL):
        self.cache_size = cache_size
        self.ttl = ttl
        self.access_denied_cache = Cache(maxsize=self.cache_size, ttl=self.ttl)

    def put_value_in_cache(self, key, value):
        return self.access_denied_cache.set(key, value)

    def get_value_from_cache(self, key):
        return self.access_denied_cache.get(key)
