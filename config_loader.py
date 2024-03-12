from line_utils_commons.enums.config import CoreConfigEnum
from line_utils_commons.clients.dynamo.core_config_table import get_core_config
from line_utils_commons.factory.singleton import Singleton
# from line_utils_commons import logging
from line_utils_commons.exception import LineDataServiceError, LineDataNotFoundError

# logger = logging.getLogger(__name__)


class CoreConfigLoaderCache(object):
    """ Simulate a connection that sends messages to a host.
    Note that each host passed to the constructor will
    instantiate this class only once."""

    __metaclass__ = Singleton

    def __init__(self):
        self._core_configs = {}
        self._core_config_keys = []

    def get_config_object(self, key, sub_key='latest'):
        lookup_key = f'{key}-{sub_key}'
        if lookup_key in self._core_config_keys:
            config = self._core_configs[lookup_key]
        else:
            try:
                config = get_core_config(key, sub_key)
            except LineDataServiceError as data_service_error:
                print(data_service_error)
                # logger.error(data_service_error)
                return None
            self._core_configs[lookup_key] = config
            self._core_config_keys.append(lookup_key)
        return config

    def get_config_value(self, key, sub_key='latest'):
        config = self.get_config_object(str(key), str(sub_key))
        # return config['configValue']
        if config:
            return config.get('configValue', None)


if __name__ == '__main__':
    config_loader_cache = CoreConfigLoaderCache()
    value = config_loader_cache.get_config(
        CoreConfigEnum.KEY_APP_SYNC,
        CoreConfigEnum.SUB_KEY_API_KEY
    )
    # logger.debug(value)
    value = config_loader_cache.get_config(
        CoreConfigEnum.KEY_APP_SYNC,
        CoreConfigEnum.SUB_KEY_API_KEY
    )
    # logger.debug(value)
    value = config_loader_cache.get_config(
        CoreConfigEnum.KEY_APP_SYNC,
        CoreConfigEnum.SUB_KEY_API_KEY
    )
    # logger.debug(value)