"""
SQLHelpersAJM.py

classes meant to streamline interaction with multiple different flavors of SQL database including MSSQL and SQLlite

"""
from abc import abstractmethod
from collections import ChainMap
from typing import Optional, List, Union
import logging

from _backend import deprecated, _NoCursorInitializedError, _NoResultsToConvertError
from _version import __version__


class _BaseSQLHelper:
    """
    _BaseSQLHelper is an abstract base class providing database connection, querying,
        and result transformation capabilities. This class includes methods for managing database connections,
        querying data, and processing query results. It uses a logger for debugging and error handling,
        and supports methods for obtaining query results in different formats.

    Methods:

    __init__(**kwargs)
        Initializes the class instance with optional logging and prepares placeholders for connection, cursor, and query results.

    is_ready_for_query
        Checks if the cursor object is available, indicating readiness to execute queries.

    _connect()
        Abstract method for establishing a connection to a database.

    log_and_raise_error(err: Exception)
        Logs an error and raises the same exception.

    GetConnectionAndCursor()
        Deprecated method for obtaining the database connection and cursor. Use get_connection_and_cursor() instead.

    get_connection_and_cursor()
        Establishes a database connection and sets up the cursor. Returns both the connection and cursor objects.

    cursor_check()
        Verifies if the cursor is initialized and ready for query execution. Raises an error if it is not.

    Query(sql_string: str, **kwargs)
        Deprecated method for querying the database. Use query() instead.

    query(sql_string: str, **kwargs)
        Executes a SQL query, retrieves the results, and stores them in the query_results attribute.

    query_results
        Property for getting and setting the results from a database query. Getter returns the stored query results,
            while setter allows updating or clearing the results.

    list_dict_results
        Converts query results into a list of dictionaries.

    results_column_names
        Provides the column names corresponding to the query results based on cursor description.

    _ConvertToFinalListDict(results: List[tuple])
        Converts a list of tuples into a sorted list of dictionaries, mapping each tuple's values
            to its corresponding column names. Raises an error if results_column_names is not available.
    """

    def __init__(self, **kwargs):
        self._logger = self._setup_logger()
        self._connection, self._cursor = None, None
        self._query_results = None
        if self._logger:
            self._logger.info(f"initialized {self.__class__.__name__} v{self.__version__}")

    @property
    def __version__(self):
        return __version__

    def _setup_logger(self, ** kwargs) -> logging.Logger:
        """
        Sets up and returns a logger for the class.

        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.hasHandlers():
            logging.basicConfig(level=kwargs.get('basic_config_level', logging.INFO))
        return logger

    @property
    def is_ready_for_query(self):
        """
        Determines if the instance is ready to execute a query.

        :return: True if the cursor object exists, otherwise False
        :rtype: bool
        """
        if hasattr(self, '_cursor') and self._cursor:
            return True
        return False

    @abstractmethod
    def _connect(self):
        """
        Establishes a connection to a database using the specified connection string.

        :return: A connection object if the connection is successful.
        :rtype: Any
        """

    def log_and_raise_error(self, err: Exception):
        """
        Logs an error message and raises the given exception.

        :param err: The exception to be logged and raised.
        :type err: Exception
        :return: None
        :rtype: None
        """
        self._logger.error(err, exc_info=True)
        raise err from None

    @deprecated(
        "This method is deprecated and will be removed in a future release. "
        "Please use the get_connection_and_cursor method instead.")
    def GetConnectionAndCursor(self):
        """
        :return: A tuple containing a database connection object and a cursor object.
        :rtype: tuple

        """
        return self.get_connection_and_cursor()

    def get_connection_and_cursor(self):
        """
        Establishes and retrieves a database connection and its associated cursor object.

        :return: A tuple containing the database connection and the cursor object
        :rtype: tuple
        """
        try:
            self._logger.debug("getting connection and cursor")
            self._connection = self._connect()
            self._cursor = self._connection.cursor()
            self._logger.debug("fetched connection and cursor")
            return self._connection, self._cursor
        except Exception as e:
            self.log_and_raise_error(e)
            return None, None
    def cursor_check(self):
        """
        Checks if the cursor is properly initialized and ready for executing queries.
        If the cursor is not initialized, it raises a `NoCursorInitializedError`, logs the error, and rethrows it.

        :return: None
        :rtype: None

        """
        if not self.is_ready_for_query:
            try:
                raise _NoCursorInitializedError()
            except _NoCursorInitializedError as e:
                self._logger.error(e, exc_info=True)
                raise e from None

    @deprecated(
        "This method is deprecated and will be removed in a future release. "
        "Please use the query method instead.")
    def Query(self, sql_string: str, **kwargs):
        """
        :param sql_string: The SQL query string to be executed.
        :type sql_string: str
        :param kwargs: Additional keyword arguments to customize the query operation.
        :type kwargs: dict
        :return: None
        :rtype: None

        """
        self.query(sql_string, **kwargs)

    @staticmethod
    def normalize_single_result(result) -> (
    Union[Optional[tuple], Optional[list], Optional[dict], Optional[str], Optional[int]]):
        """
        :param result: The input data that can be a tuple, list, or other iterable structure,
        typically containing one or more elements; used to normalize to a simpler format.

        :return: Returns a normalized result, which can be a single element or the processed input.
        The returned value can be one of tuple, list, dict, string, or integer, based on input and processing logic.
        If the input has a single element, it simplifies to that element.
        If the input’s second value is blank, simplifies further.
        :rtype: Union[Optional[tuple], Optional[list], Optional[dict], Optional[str], Optional[int]]
        """
        if len(result) == 1:
            result = result[0]
            # if the result is still one entry or the second entry of the result is blank
            if len(result) == 1 or (len(result) == 2 and result[1] == ''):
                result = result[0]
        return result

    def query(self, sql_string: str, **kwargs):
        """
        :param sql_string: The SQL query string to be executed.
        :type sql_string: str
        :return: None
        :rtype: None
        """
        is_commit = kwargs.get('is_commit', False)
        try:
            self.cursor_check()
            self._cursor.execute(sql_string)

            res = self._cursor.fetchall()
            if is_commit:
                self._logger.info("committing changes")
                self._connection.commit()
            if res:
                self._logger.info(f"{len(res)} item(s) returned.")
                print(f"{len(res)} item(s) returned.")
            else:
                if not is_commit:
                    self._logger.warning("query returned no results")
            res = self.normalize_single_result(res)
            self.query_results = res
        except Exception as e:
            self.log_and_raise_error(e)

    @property
    def query_results(self) -> Optional[List[tuple]]:
        """
        :return: The query results stored in the object. Returns a list of tuples or None if no results are available.
        :rtype: Optional[List[tuple]]

        """
        return self._query_results

    @query_results.setter
    def query_results(self, value: List[dict] or None):
        """
        :param value: The list of dictionaries containing query results or None to reset the results.
        :type value: List[dict] or None
        """
        self._query_results = value

    @property
    def list_dict_results(self):
        """
        Returns the processed query results converted into a list of dictionaries.

        :return: A list of dictionaries obtained from the processed query results, or None if no query results are available.
        :rtype: list[dict] or None
        """
        if self.query_results:
            return self._ConvertToFinalListDict(self.query_results)
        return None

    @property
    def results_column_names(self) -> List[str] or None:
        """
        :return: A list of column names of the results from the cursor description, or None if the cursor description is not available.
        :rtype: List[str] or None
        """
        try:
            return [d[0] for d in self._cursor.description]
        except AttributeError:
            return None

    def _ConvertToFinalListDict(self, results: List[tuple]) -> List[dict] or None:
        """
        Converts a list of tuples into a list of dictionaries. This method maps each tuple's values to its corresponding column names contained
        in the `self.results_column_names` attribute. If the attribute is not set, an AttributeError is raised. The method also ensures that the
        final output is sorted by key for each dictionary in the list.

        :param results: A list of tuples where each tuple represents a row of data.
        :type results: List[tuple]
        :return: A sorted list of dictionaries, where each dictionary corresponds to a row of data, or None if no valid data exists.
        :rtype: List[dict] or None
        """
        row_list_dict = []
        final_list_dict = []

        for row in results:
            if self.results_column_names:
                for cell, col in zip(row, self.results_column_names):
                    row_list_dict.append({col: cell})
                final_list_dict.append(dict(ChainMap(*row_list_dict)))
                row_list_dict.clear()
            else:
                raise _NoResultsToConvertError()
        if len(final_list_dict) > 0:
            # this returns a sorted list dict instead of an unsorted list dict
            return [dict(sorted(x.items())) for x in final_list_dict]
        return None


class _BaseConnectionAttributes(_BaseSQLHelper):
    """
    A base class for managing database connection attributes, constructing connection strings,
    and providing mechanisms to populate class attributes either through explicit arguments
    or a provided connection string.

    Constants:
    - TRUSTED_CONNECTION_DEFAULT: Default value for trusted connection, set to 'yes'.
    - DRIVER_DEFAULT: Default driver, set to None.
    - INSTANCE_DEFAULT: Default instance, set to 'SQLEXPRESS'.

    Methods:
    - __init__: Initializes the class and assign connection attributes.
    - connection_information: Property returning a dictionary with connection details, excluding actual password values.
    - connection_string: Property that constructs and returns the connection string for connecting to the database.
    - _connection_string_to_attributes: Static method that parses a given connection string into individual attributes.
    - with_connection_string: Class method for creating an instance of the class by parsing and using a connection string.

    Initialization Parameters:
    - server: The database server address. Required.
    - database: The name of the database. Required.
    - instance: The name of the database instance. Defaults to 'SQLEXPRESS'.
    - driver: The database driver. Defaults to None.
    - trusted_connection: Specifies if a trusted connection is used. Defaults to 'yes'.
    - kwargs: Additional optional parameters, including 'logger', 'connection_string', 'username', and 'password'.
    """
    TRUSTED_CONNECTION_DEFAULT = 'yes'
    DRIVER_DEFAULT = None
    INSTANCE_DEFAULT = 'SQLEXPRESS'

    def __init__(self, server, database, instance=INSTANCE_DEFAULT, driver=DRIVER_DEFAULT,
                 trusted_connection=TRUSTED_CONNECTION_DEFAULT, **kwargs):
        super().__init__(**kwargs)
        self._connection_string = kwargs.get('connection_string', None)

        if self._connection_string is not None:
            self._logger.debug("populating class attributes "
                               "using the provided connection string")
            self.__class__.with_connection_string(self._connection_string, logger=self._logger)

        self.server = server
        self.instance = instance
        self.database = database
        self.driver = driver
        self.username = kwargs.get('username', '')
        self._password = kwargs.get('password', '')
        self.port = kwargs.get('port', 0)
        self.trusted_connection = trusted_connection

        if all(self.connection_information):
            self._logger.debug(f"initialized {self.__class__.__name__} with the following connection parameters:\n"
                               f"{', '.join(['='.join(x) for x in self.connection_information.items()])}")
            self._logger.info(f"initialized {self.__class__.__name__}")

    @abstractmethod
    def _connect(self):
        """
        Establishes a connection to a database using the specified connection string.

        :return: A connection object if the connection is successful.
        :rtype: Any
        """

    @property
    def connection_information(self):
        """
        :return: A dictionary containing the connection information including server, instance,
        database, driver, username, a placeholder for the password ('WITHHELD or None'), and trusted_connection status.
        :rtype: dict
        """
        return {'server': self.server,
                'instance': self.instance,
                'database': self.database,
                'driver': self.driver,
                'username': self.username,
                'password': 'WITHHELD or None',
                'trusted_connection': self.trusted_connection}

    @property
    def connection_string(self):
        """
        Constructs and returns the connection string if required attributes are provided.

        :return: The constructed connection string composed of driver, server, and database information.
        :rtype: str
        """
        if all((self.server, self.instance, self.database, self.driver)):
            self._connection_string = (f"driver={self.driver};"
                                       f"server={self.server}\\{self.instance};"
                                       f"database={self.database};"
                                       f"UID={self.username};"
                                       f"PWD={self._password};"
                                       f"trusted_connection={self.trusted_connection}")
            # self._logger.debug(
            #     f"populated connection string as {self._connection_string}")
        return self._connection_string

    @staticmethod
    def _connection_string_to_attributes(connection_string: str,
                                         attr_split_char: str,
                                         key_value_split_char: str):
        """
        :param connection_string: The connection string containing attributes to be split and parsed.
        :type connection_string: str
        :param attr_split_char: The character used to split the connection string into individual attributes.
        :type attr_split_char: str
        :param key_value_split_char: The character used to separate keys from values in each attribute.
        :type key_value_split_char: str
        :return: A dictionary of parsed key-value pairs from the connection string. If a 'server'
            attribute includes an instance, it will be split into separate 'server' and 'instance' keys.
        :rtype: dict
        """
        cxn_attrs = connection_string.split(attr_split_char)
        cxn_attrs = {x.split(key_value_split_char)[0].lower(): x.split(key_value_split_char)[1] for x in cxn_attrs}
        if len(cxn_attrs.get('server').split('\\')) == 2:
            cxn_attrs.update({'server': cxn_attrs.get('server').split('\\')[0],
                              'instance': cxn_attrs.get('server').split('\\')[1]})
        return cxn_attrs

    @classmethod
    def with_connection_string(cls, connection_string: str,
                               attr_split_char: str = ';', key_value_split_char: str = '=', **kwargs):
        """
        :param connection_string: A string containing the connection attributes separated by attr_split_char
            and key-value pairs separated by key_value_split_char. This parameter is mandatory and cannot be None.
        :type connection_string: Optional[str]
        :param attr_split_char: The character used to split the connection attributes in the connection_string.
            Default is ';'.
        :type attr_split_char: str
        :param key_value_split_char: The character used to separate keys and values in each connection attribute
            in the connection_string. Default is '='.
        :type key_value_split_char: str
        :param kwargs: Additional keyword arguments to be passed during the initialization of the class.
        :return: An instance of the class initialized with the attributes parsed from the connection_string
            and additional keyword arguments.
        :rtype: cls
        """
        if not connection_string:
            raise AttributeError("connection_string is required")
        cxn_attrs = cls._connection_string_to_attributes(connection_string,
                                                         attr_split_char,
                                                         key_value_split_char)
        return cls(**cxn_attrs, **kwargs)

