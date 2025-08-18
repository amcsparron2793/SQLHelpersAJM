import logging
from typing import Optional, List
import pyodbc

from SQLHelpersAJM import _BaseSQLHelper


class SQLServerHelper(_BaseSQLHelper):
    """
    A helper class to interact with an SQL Server database using pyodbc. Provides utility methods for connection management, executing queries, and formatting query results.

    class SQLServerHelper:
        __init__(self, server=None, database=None, driver='{SQL Server}', **kwargs):
            Initializes the SQLServerHelper instance with optional server, database, and driver attributes.
            Optionally uses a provided connection string to populate attributes.

            :param server: The SQL Server address. Defaults to None.
            :type server: str, optional
            :param database: The database name. Defaults to None.
            :type database: str, optional
            :param driver: The name of the ODBC driver. Defaults to '{SQL Server}'.
            :type driver: str, optional
            :param kwargs: Additional attributes, including a logger or a connection string.

        connection_string
            Constructs and returns the connection string if required attributes are provided.

            :return: The constructed connection string composed of driver, server, and database information.
            :rtype: str

        is_ready_for_query
            Determines if the instance is ready to execute a query.

            :return: True if the cursor object exists, otherwise False
            :rtype: bool

        with_connection_string(cls, connection_string: Optional[str], attr_split_char: str = ';', key_value_split_char: str = '=', **kwargs):
            Initializes a class instance by extracting attributes from a provided connection string. Raises an error if the connection string is not provided.

            :param connection_string: A string containing the connection attributes separated by attr_split_char and key-value pairs separated by key_value_split_char. This parameter is mandatory and cannot be None.
            :type connection_string: Optional[str]
            :param attr_split_char: The character used to split the connection attributes in the connection_string. Default is ';'.
            :type attr_split_char: str
            :param key_value_split_char: The character used to separate keys and values in each connection attribute in the connection_string. Default is '='.
            :type key_value_split_char: str
            :param kwargs: Additional keyword arguments to be passed during the initialization of the class.
            :return: An instance of the class initialized with the attributes parsed from the connection_string and additional keyword arguments.
            :rtype: cls

        cursor_check(self):
            Checks if the cursor is properly initialized and ready for executing queries. If the cursor is not initialized, it raises a `NoCursorInitializedError`, logs the error, and rethrows it.

            :return: None
            :rtype: None

        get_connection_and_cursor(self):
            Establishes and retrieves a database connection and its associated cursor object.

            :return: A tuple containing the database connection and the cursor object
            :rtype: tuple

        _connect(self):
            Establishes a connection to a database using the specified connection string.

            :return: A connection object if the connection is successful.
            :rtype: pyodbc.Connection
            :raises pyodbc.Error: If there is an error while attempting to connect to the database.

        query(self, sql_string: str):
            Executes the provided SQL query string after initializing the cursor.

            :param sql_string: The SQL query string to be executed.
            :type sql_string: str
            :return: None
            :rtype: None

        query_results(self) -> Optional[List[tuple]]:
            Retrieves the query results stored in the object.

            :return: The query results as a list of tuples or None if no results are available.
            :rtype: Optional[List[tuple]]

        query_results(self, value: List[dict] or None):
            Sets the query results stored in the object.

            :param value: The list of dictionaries containing query results or None to reset the results.
            :type value: List[dict] or None

        list_dict_results(self):
            Converts the query results into a list of dictionaries.

            :return: A list of dictionaries obtained from the processed query results, or None if no query results are available.
            :rtype: list[dict] or None

        results_column_names(self) -> List[str] or None:
            Retrieves the column names of the query results if available.

            :return: A list of column names or None if the cursor description is not available.
            :rtype: List[str] or None

        _ConvertToFinalListDict(self, results: List[tuple]) -> List[dict] or None:
            Converts a list of tuples representing query results into a list of dictionaries. Maps each tuple's values to its corresponding column names.

            :param results: A list of tuples where each tuple represents a row of data.
            :type results: List[tuple]
            :return: A sorted list of dictionaries, where each dictionary corresponds to a row of data, or None if no valid data exists.
            :rtype: List[dict] or None
    """
    TRUSTED_CONNECTION_DEFAULT = 'yes'
    DRIVER_DEFAULT = '{SQL Server}'

    def __init__(self, server=None, database=None, driver=DRIVER_DEFAULT,
                 trusted_connection=TRUSTED_CONNECTION_DEFAULT, **kwargs):
        self._logger = kwargs.get('logger', logging.getLogger(__name__))
        super().__init__(**kwargs)

        self._connection_string = kwargs.get('connection_string', None)
        if self._connection_string is not None:
            self._logger.debug("populating server, database, and driver attributes "
                               "using the provided connection string")
            self.__class__.with_connection_string(self._connection_string, logger=self._logger)

        self.server = server
        self.database = database
        self.driver = driver
        self.trusted_connection = trusted_connection
        self._connection, self._cursor = None, None
        self._query_results = None

        if all(self.connection_information):
            self._logger.info(f"initialized {self.__class__.__name__} with the following connection parameters"
                              f"{self.connection_information}")

    @property
    def connection_information(self):
        return self.server, self.database, self.driver, self.trusted_connection

    @property
    def connection_string(self):
        """
        Constructs and returns the connection string if required attributes are provided.

        :return: The constructed connection string composed of driver, server, and database information.
        :rtype: str
        """
        if all((self.server, self.database, self.driver, self.trusted_connection)):
            self._connection_string = (f"driver:{self.driver};"
                                       f"server={self.server};"
                                       f"database={self.database};"
                                       f"trusted_connection={self.trusted_connection}")
            self._logger.debug(f"populated connection string as {self._connection_string}")
        return self._connection_string

    @classmethod
    def with_connection_string(cls, connection_string: str,
                               attr_split_char: str = ';', key_value_split_char: str = '=', **kwargs):
        """
        :param connection_string: A string containing the connection attributes separated by attr_split_char and key-value pairs separated by key_value_split_char. This parameter is mandatory and cannot be None.
        :type connection_string: Optional[str]
        :param attr_split_char: The character used to split the connection attributes in the connection_string. Default is ';'.
        :type attr_split_char: str
        :param key_value_split_char: The character used to separate keys and values in each connection attribute in the connection_string. Default is '='.
        :type key_value_split_char: str
        :param kwargs: Additional keyword arguments to be passed during the initialization of the class.
        :return: An instance of the class initialized with the attributes parsed from the connection_string and additional keyword arguments.
        :rtype: cls
        """
        if not connection_string:
            raise AttributeError("connection_string is required")
        cxn_attrs = connection_string.split(attr_split_char)
        cxn_attrs = {x.split(key_value_split_char)[0].lower(): x.split(key_value_split_char)[1] for x in cxn_attrs}
        return cls(**cxn_attrs, **kwargs)

    def _connect(self):
        """
        Establishes a connection to a database using the specified connection string.

        :return: A connection object if the connection is successful.
        :rtype: pyodbc.Connection
        :raises pyodbc.Error: If there is an error while attempting to connect to the database.
        """
        cxn = pyodbc.connect(self.connection_string)
        self._logger.debug("connection successful")
        return cxn


if __name__ == '__main__':
    SQLServerHelper.with_connection_string("driver={SQL Server};server=localhost;database=test;trusted_connection=yes")