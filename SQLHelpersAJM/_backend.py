import functools
import warnings


def deprecated(reason: str = ""):
    """
    Decorator that marks a function or method as deprecated.

    :param reason: Optional message to explain what to use instead
                   or when the feature will be removed.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            message = f"Function '{func.__name__}' is deprecated."
            if reason:
                message += f" {reason}"
            warnings.warn(message, category=DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    return decorator


class _UseDefaultMessageBase(Exception):
    DEFAULT_MESSAGE = ""

    def __init__(self, msg: str = None):
        if not msg:
            msg = self.__class__.DEFAULT_MESSAGE
        super().__init__(msg)


class _MissingRequiredClassAttribute(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = "Missing at least one required class attribute"


class _NoTrackedTablesError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("No tables have been specified to track. "
                       "Please specify tables to track in the TABLES_TO_TRACK class variable.")


class _NoCursorInitializedError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("Cursor has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class _NoConnectionInitializedError(_NoCursorInitializedError):
    DEFAULT_MESSAGE = ("Connection has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class _NoResultsToConvertError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("A query has not been executed, "
                       "please execute a query before calling this method.")
