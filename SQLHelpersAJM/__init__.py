import logging
from logging import getLogger, basicConfig, INFO, Logger
from typing import Optional

# FIXME: MAKE SURE SQLHELPERSAJM doesn't reinitialize the logger when it doesnt need to!!!!
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
        """
        Validates the 'basic_config_level' (bcl) input passed through the keyword arguments.

        :param kwargs: Arbitrary keyword arguments containing potential 'basic_config_level'.
        :type kwargs: dict
        :return: The value of 'basic_config_level' if valid; False otherwise.
        :rtype: str or int or False
        """
        bcl = kwargs.get('basic_config_level')
        if (bcl in logging.getLevelNamesMapping().keys()
                or bcl in logging.getLevelNamesMapping().values()):
            return bcl
        return False

    def _get_bcl(self, **kwargs):
        """
        :param kwargs: Keyword arguments that may contain 'logger' for logging and 'basic_config_level'
            for specifying a configuration level.
        :type kwargs: dict
        :return: The validated or default basic configuration level (bcl).
        :rtype: Any
        """
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
        :param kwargs: A dictionary of optional keyword arguments.
                        - 'logger': An instance of a Logger. If provided, this logger will be used.
                        - 'logger_name_to_get': A string representing the name of the logger to be retrieved.
                            Cannot be used together with 'logger'.
                        - 'skip_basic_config': A boolean indicating whether to skip basic configuration for the logger.
                            Default is False.
        :type kwargs: dict
        :return: Configured logger instance.
        :rtype: Logger
        :raises ValueError: If both 'logger' and 'logger_name_to_get' are provided in kwargs.
        """
        def _eval_kwargs():
            if kwargs.get('logger') and kwargs.get('logger_name_to_get'):
                raise ValueError("Cannot specify both logger and logger_name_to_get.")

            logger_name_to_get = kwargs.get('logger_name_to_get', self.__class__.__name__)
            lg: Logger = kwargs.get('logger', getLogger(logger_name_to_get))

            sbc = kwargs.get('skip_basic_config', False)
            return lg, sbc

        logger, skip_basic_config = _eval_kwargs()

        if not skip_basic_config:
            if not logger.hasHandlers():
                # noinspection PyTypeChecker
                bcl = self._get_bcl(logger=logger, **kwargs)
                basicConfig(level=bcl)
        return logger


from SQLHelpersAJM import backend, helpers

__all__ = ['backend', 'helpers']