from logging import getLogger, basicConfig, INFO, Logger


class _SharedLogger:
    """
    Class for managing shared logger instances.

    Methods:
        _setup_logger(**kwargs):
            Sets up and returns a logger instance for the class.
    """
    def _setup_logger(self, **kwargs) -> Logger:
        """
        Sets up and returns a logger for the class.

        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        logger = getLogger(self.__class__.__name__)
        if not logger.hasHandlers():
            basicConfig(level=kwargs.get('basic_config_level', INFO))
        return logger


from SQLHelpersAJM.bases import BaseSQLHelper, BaseConnectionAttributes, BaseCreateTriggers
from SQLHelpersAJM.SQLServer import SQLServerHelper
from SQLHelpersAJM.SQLite3_helper import SQLite3Helper, SQLite3HelperTT
