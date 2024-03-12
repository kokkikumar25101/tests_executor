from line_utils_commons import logging

logger = logging.getLogger(__name__)


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


def print_invoked(key):
    logger.debug(f'invoked {key}')


class ConfigLoaderCache(metaclass=SingletonMeta):


    _configs = {}
    _config_keys = []

    def get_config(self, key, sub_key='latest'):
        config = None
        if key in self._config_keys:
            config = self._configs[key]
        else:
            # config = get_core_config(key, sub_key)
            config = "tss"
            print_invoked(key)
            self._configs[key] = config
            self._config_keys.append(key)

        return config


class UserConfigLoaderCache(metaclass=SingletonMeta):
    def some_business_logic(self):
        """
        Finally, any singleton should define some business logic, which can be
        executed on its instance.
        """

        # ...

if __name__ == "__main__":
    # The client code.

    s1 = ConfigLoaderCache()
    s2 = UserConfigLoaderCache()

    if id(s1) == id(s2):
        logger.debug("Singleton works, both variables contain the same instance.")
    else:
        logger.debug("Singleton failed, variables contain different instances.")

    s1.get_config("Test")
    s1.get_config("Test")
    s1.get_config("Test1")