import logging
import sys
from typing import cast
import colorlog
import os

log_colors = {
    "DEBUG": "blue",
    "INFO": "green",
    "WARNING": "bold_yellow",
    "ERROR": "bold_red",
    "CRITICAL": "bold_red",
}

ENV_LOG_FILE_NAME = "DATAFLOW_LOG_FILE"


class Logger(logging.Logger):
    """This class is used for logging purposes for entirity of this package"""

    default_level = logging.INFO
    _instances = []

    def __new__(cls, *args, **kwargs):

        instance = super().__new__(cls)
        cls._instances.append(instance)
        return instance

    def __init__(self, name, log_file=None, stream=True):
        """
        This method initializes the logger class

        Parameters
        ------------------
        name: str
            name of the module at which logging should be initialized

        """
        super().__init__(name)
        self._level_changed = False
        self.setLevel(Logger.default_level)
        log_file = (
            log_file
            if log_file is not None
            else os.environ.get(ENV_LOG_FILE_NAME, None)
        )
        if stream:
            self.set_stream_handler()
        if log_file is not None:
            self.set_file_handler(log_file)

    def setLevel(self, level: str, root: bool = False):
        """
        Set the logging level of this logger.  level must be an int or a str.
        """
        super().setLevel(level)
        if root:
            self._level_changed = True

    @classmethod
    def set_level(cls, level):
        for obj in cls._instances:
            if not obj._level_changed:
                obj.setLevel(level, True)
        cls.default_level = level

    @classmethod
    def get_level(cls):
        return cls.default_level

    @property
    def get_level_formatter(self):
        """This property used for setting the formatter used for the logging session based on level"""
        logging_format_messages = {
            "DEBUG": "%(log_color)s%(levelname)s::%(asctime)s::%(name)s::%(module)s::%(funcName)s::%(lineno)d:: %(white)s%(message)s",
            "INFO": "%(log_color)s%(levelname)s::%(asctime)s::%(name)s:: %(white)s%(message)s",
            "WARNING": "%(log_color)s%(levelname)s::%(asctime)s::%(name)s::%(message)s",
            "ERROR": "%(log_color)s%(levelname)s::%(asctime)s::%(name)s::%(message)s",
            "CRITICAL": "%(log_color)s%(levelname)s::%(asctime)s::%(name)s::%(message)s",
        }
        return logging_format_messages

    def set_stream_handler(self):
        """This handler will be used to set the stream handler to send the log messages to stdout"""
        # Create a console handler
        console_handler = logging.StreamHandler(sys.stdout)
        # Create a formatter and set it for the handlers
        formatter = colorlog.LevelFormatter(
            self.get_level_formatter, log_colors=log_colors
        )

        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        self.addHandler(console_handler)

    def set_file_handler(self, log_file):
        """This method will sets the file handler and pass the log messages to file"""
        # Create a file handler
        file_handler = logging.FileHandler(log_file)

        formatter = colorlog.ColoredFormatter(
            self.get_level_formatter,
            log_colors=colorlog.default_log_colors,
            force_color=True,
        )
        file_handler.setFormatter(formatter)
        # Add handlers to the logger
        self.addHandler(file_handler)

    @classmethod
    def get_logger(cls, name: str) -> "Logger":
        """Get a Dataflow Logger instance or create a new one.

        Parameters
        ----------
        name : str, optional
            The name of the logger.

        Returns
        -------
        Logger
            A configured instance of Logger.
        """
        existing_logger = logging.getLoggerClass()
        if not isinstance(existing_logger, Logger):
            logging.setLoggerClass(Logger)

        logger = logging.getLogger(name)
        # Reset to the existing logger
        logging.setLoggerClass(existing_logger)

        return cast(Logger, logger)
