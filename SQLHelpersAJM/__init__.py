import logging
from logging import getLogger, basicConfig, INFO, Logger
from typing import Optional


class _SharedLogger:
    """
    Class for managing shared logger instances.

    Methods:
        _setup_logger(**kwargs):
            Sets up and returns a logger instance for the class.
    """
    _DEFAULT_BCL = INFO

    @staticmethod
    def _validate_bcl(**kwargs):
        bcl = kwargs.get('basic_config_level')
        if (bcl in logging.getLevelNamesMapping().keys()
                or bcl in logging.getLevelNamesMapping().values()):
            return bcl
        return False

    def _get_bcl(self, **kwargs):
        bcl = None
        logger: Optional[Logger] = kwargs.get('logger', None)
        if kwargs.get('basic_config_level'):
            bcl = self._validate_bcl(**kwargs)
        if not bcl:
            bcl = self.__class__._DEFAULT_BCL
            if logger:
                logger.info(f"Basic config level not set. Defaulting to {bcl}.")
        return bcl

    def _setup_logger(self, **kwargs) -> Logger:
        """
        Sets up and returns a logger for the class.

        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        logger = getLogger(self.__class__.__name__)
        if not logger.hasHandlers():
            bcl = self._get_bcl(logger=logger, **kwargs)
            basicConfig(level=bcl)
        return logger


from SQLHelpersAJM import backend, helpers

__all__ = ['backend', 'helpers']