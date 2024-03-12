from line_utils_commons.enums.config import CoreConfigEnum
from line_utils_commons.factory.singleton import Singleton
from line_utils_commons.exception import LineDataServiceError


class CacheFactory(object):
    """ Simulate a connection that sends messages to a host.
    Note that each host passed to the constructor will
    instantiate this class only once."""

    __metaclass__ = Singleton

    def __init__(self):
        self._cache_base = {}

    def get_keys(self):
        return list(self._cache_base.keys())

    def get_sub_keys(self, base_key):
        if base_key in self._cache_base:
            cache_obj = self._cache_base[base_key]
            return list(cache_obj.keys())
        else:
            raise LineDataServiceError(f"Base Key {base_key} does not exist")

    def get_item(self, base_key, sub_key='latest', delete_key=False):
        if base_key in self._cache_base:
            cache_obj = self._cache_base[base_key]
            if sub_key in cache_obj:
                if delete_key:
                    cache_value = cache_obj.pop(sub_key)
                else:
                    cache_value = cache_obj[sub_key]
                return cache_value
            else:
                raise LineDataServiceError(f"Sub Key {sub_key} does not exist")
        else:
            raise LineDataServiceError(f"Base Key {base_key} does not exist")

    def set_item(self, base_key, sub_key='latest', cache_value=None):
        if base_key in self._cache_base:
            cache_obj = self._cache_base[base_key]
            cache_obj[sub_key] = cache_value
            self._cache_base[base_key] = cache_obj
        else:
            self._cache_base[base_key] = {
                sub_key: cache_value
            }


if __name__ == '__main__':
    cache = CacheFactory()
    cache.set_item("LIVE_DATA", "OPTIDX", 200)
    cache.set_item("LIVE_DATA", "OPTIDX1", 300)

    cache.set_item("LIVE_DATA_2", "OPTIDX1", 300)

    value = cache.get_item("LIVE_DATA", "OPTIDX1")
    print(value)
    value = cache.get_item("LIVE_DATA", "OPTIDX")
    print(value)