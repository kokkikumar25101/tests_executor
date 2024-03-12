import os
from line_utils_commons.factory.singleton import Singleton
from line_utils_commons.config import getLineCommonProperties
import logging

line_common_properties = getLineCommonProperties()

logger_level = os.environ.get(
    'LINE_COMMONS_LOG_LEVEL',
    line_common_properties.get('LINE_COMMONS_LOG_LEVEL', 'INFO')).upper()
enable_console_logging = os.environ.get(
    'LINE_CONSOLE_LOG_ENABLE',
    line_common_properties.get('LINE_CONSOLE_LOG_ENABLE', 'False')).lower() in ('true', '1', 't')
log_formatter_disable = os.environ.get(
    'LINE_CONSOLE_LOG_FORMATTER_DISABLE',
    line_common_properties.get('LINE_CONSOLE_LOG_FORMATTER_DISABLE', 'False')).lower() in ('true', '1', 't')
log_formatter_append_filename = os.environ.get(
    'LINE_CONSOLE_LOG_FORMATTER_APPEND_FILENAME',
    line_common_properties.get('LINE_CONSOLE_LOG_FORMATTER_APPEND_FILENAME', 'True')).lower() in ('true', '1', 't')


class LineLogger(object):
    """
    Wraps the Python logging module's logger object to ensure that all
    logging happens with the correct configuration as well as any extra
    information that might be required by the log file (for example, the user
    on the machine, hostname, IP address lookup, etc).

    Subclasses must specify their logger as a class variable so all instances
    have access to the same logging object. Basic Usage:
    """

    def __init__(self, logger_name=None, extra={}):

        self.logger_name = logger_name
        self.line_extra = self.get_line_extra_format(extra)
        # self.logger_level = logger_level
        # self.enable_console_logging = enable_console_logging
        # self.logger_name = kwargs.pop('logger_name', None)
        # logger_level = kwargs.pop('logger_level', None)
        # enable_console_logging = kwargs.pop('enable_console_logging', True)

        if not self.logger_name:
            raise ValueError('logger_name is required')

        self.logger = logging.getLogger(self.logger_name)
        # logging.INFO
        self.logger.setLevel(logger_level)

        for handler in self.logger.handlers:
            handler.setLevel(logger_level)

        if enable_console_logging:
            console_handler = logging.StreamHandler()
            if not log_formatter_disable:

                if log_formatter_append_filename:
                    log_formatter = logging.Formatter(f"%(asctime)s%(lineextrastr)s [{self.logger_name}] [%(levelname)-5.5s]  %(message)s")
                else:
                    log_formatter = logging.Formatter(f"%(asctime)s%(lineextrastr)s [%(levelname)-5.5s]  %(message)s")
                console_handler.setFormatter(log_formatter)

            console_handler.setLevel(logger_level)
            self.logger.addHandler(console_handler)

        self.extra = extra

    # def get_logger_level(self):
    #     return self.logger_level
    #
    # def is_console_logging_enabled(self):
    #     return self.enable_console_logging

    def get_line_extra_format(self, extra):
        extras_formatter = ""
        if extra:
            for extra_key in extra.keys():
                extras_formatter = extras_formatter + f" {extra[extra_key]}"
        return {"lineextrastr": extras_formatter}

    def log(self, level, message, *args, **kwargs):
        """
        This is the primary method to override to ensure logging with extra
        options gets correctly specified.
        """
        extra = self.extra.copy()
        extra.update(kwargs.pop('extra', {}))
        # extra.update(self.line_extra)
        extra = {**extra, **self.line_extra}
        kwargs['extra'] = extra
        self.logger.log(level, message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        return self.log(logging.DEBUG, message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        return self.log(logging.INFO, message, *args, **kwargs)

    def warn(self, message, *args, **kwargs):
        return self.log(logging.WARN, message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        return self.log(logging.ERROR, message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        return self.log(logging.CRITICAL, message, *args, **kwargs)


class LineLoggerFactory(object):
    """ Simulate a connection that sends messages to a host.
    Note that each host passed to the constructor will
    instantiate this class only once."""

    __metaclass__ = Singleton

    def __init__(self):

        log_level_mapping = {
            'CRITICAL': 50,
            'FATAL': 50,
            'ERROR': 40,
            'WARNING': 30,
            'WARN': 30,
            'INFO': 20,
            'DEBUG': 10,
            'NOTSET': 0,
        }
        self.line_extra = {}
        self._loggers = {}

    def set_line_extra(self, extra):
        self.line_extra = extra

    def update_loggers(self, **kwargs):
        extra = kwargs['extra']
        self.set_line_extra(extra)
        for k, v in self._loggers.items():
            v.line_extra = v.get_line_extra_format({**v.extra, **extra})

    def get_logger(self, **kwargs):
        logger_name = kwargs['logger_name']
        # logger_level = kwargs.get('logger_level', None)
        # enable_console_logging = kwargs.get('enable_console_logging', None)
        if logger_name in self._loggers:
            logger_object = self._loggers[logger_name]
        else:
            # if not logger_level:
            #     kwargs['logger_level'] = self.level
            # if not enable_console_logging:
            #     kwargs['enable_console_logging'] = self.enable_console_logging

            logger_object = LineLogger(**kwargs)
            self._loggers[logger_name] = logger_object
        return logger_object


# class LineLogger(LineLogger):
#
#     logger = logging.getLogger('foo')
#
#     def __init__(self, **kwargs):
#         self._log_class = kwargs.pop('user', None)
#         super(LineLogger, self).__init__(**kwargs)
#
#     @property
#     def user(self):
#         if not self._user:
#             self._user = getpass.getuser()
#         return self._user
#
#     def log(self, level, message, *args, **kwargs):
#         """
#         Provide current user as extra context to the logger
#         """
#         extra = kwargs.pop('extra', {})
#         extra.update({
#             'user': self.user
#         })
#
#         kwargs['extra'] = extra
#         super(FooLogger, self).log(level, message, *args, **kwargs)
