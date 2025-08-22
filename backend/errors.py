class _UseDefaultMessageBase(Exception):
    DEFAULT_MESSAGE = ""

    def __init__(self, msg: str = None):
        if not msg:
            msg = self.__class__.DEFAULT_MESSAGE
        super().__init__(msg)


class MissingRequiredClassAttribute(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = "Missing at least one required class attribute"


class NoTrackedTablesError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("No tables have been specified to track. "
                       "Please specify tables to track in the TABLES_TO_TRACK class variable.")


class NoCursorInitializedError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("Cursor has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class NoConnectionInitializedError(NoCursorInitializedError):
    DEFAULT_MESSAGE = ("Connection has not been initialized yet, "
                       "run get_connection_and_cursor before querying")


class NoResultsToConvertError(_UseDefaultMessageBase):
    DEFAULT_MESSAGE = ("A query has not been executed, "
                       "please execute a query before calling this method.")
