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


class _NoCursorInitializedError(Exception):
    DEFAULT_MESSAGE = "Cursor has not been initialized yet, run get_connection_and_cursor before querying"

    def __init__(self, msg: str = None):
        if not msg:
            msg = _NoCursorInitializedError.DEFAULT_MESSAGE
        super().__init__(msg)
