from logging import getLogger, basicConfig, INFO, Logger


class _SharedLogger:
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
from SQLHelpersAJM.SQLLite3HelperClass import SQLlite3Helper, SQLite3HelperTT