class _UseDefaultMessageBase(Exception):
    """
    Base exception class with a default message.

    This class serves as a base for custom exceptions that require a default
    message if no specific message is provided during instantiation.

    Attributes:
        DEFAULT_MESSAGE (str): The default error message for the exception.

    Methods:
        __init__(msg: str = None):
            Initializes the exception instance with the provided message or
            the default message if none is provided.
    """
    DEFAULT_MESSAGE = ""

    def __init__(self, msg: str = None):
        if not msg:
            msg = self.__class__.DEFAULT_MESSAGE
        super().__init__(msg)


class MissingRequiredClassAttribute(_UseDefaultMessageBase):
    """
    A custom exception raised when a required class attribute is missing.

    This exception inherits from `_UseDefaultMessageBase` and uses
    a default message to indicate that at least one required class
    attribute is missing during class definition or usage.

    Attributes:
        DEFAULT_MESSAGE (str): The default error message used when
            this exception is raised.
    """
    DEFAULT_MESSAGE = "Missing at least one required class attribute"


class NoTrackedTablesError(_UseDefaultMessageBase):
    """
    Custom exception raised when no tables are specified to be tracked.

    This error is triggered when the `TABLES_TO_TRACK` class variable has not been
    defined or is left empty, indicating that no database tables have been marked
    for tracking.

    Attributes:
        DEFAULT_MESSAGE: Default error message to describe the issue of missing
                         tracked tables.
    """
    DEFAULT_MESSAGE = ("No tables have been specified to track. "
                       "Please specify tables to track in the TABLES_TO_TRACK class variable.")


class NoCursorInitializedError(_UseDefaultMessageBase):
    """
    Custom exception raised when an attempt is made to query without initializing the cursor.

    Attributes:
        DEFAULT_MESSAGE (str): Error message indicating the absence of cursor initialization.
    """
    DEFAULT_MESSAGE = ("Cursor has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class NoConnectionInitializedError(NoCursorInitializedError):
    """
    Custom exception raised when an operation is attempted without initializing a database connection.

    Attributes:
        DEFAULT_MESSAGE (str): Error message indicating that no connection has been initialized,
        and get_connection_and_cursor must be run before querying.
    """
    DEFAULT_MESSAGE = ("Connection has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class NoResultsToConvertError(_UseDefaultMessageBase):
    """
    A custom exception class that is raised when attempting to convert query results
    without executing a query first.

    Attributes
    ----------
    DEFAULT_MESSAGE : str
        The default error message to be displayed when the exception is raised.
    """
    DEFAULT_MESSAGE = ("A query has not been executed, "
                       "please execute a query before calling this method.")
