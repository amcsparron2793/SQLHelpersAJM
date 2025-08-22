from abc import abstractmethod
from collections import ChainMap
from typing import Union, Optional, List

from SQLHelpersAJM import _SharedLogger
from SQLHelpersAJM._version import __version__
from backend import deprecated, NoCursorInitializedError, NoResultsToConvertError, NoTrackedTablesError, \
    MissingRequiredClassAttribute


class BaseSQLHelper(_SharedLogger):
    """
    BaseSQLHelper is an abstract base class providing database connection, querying,
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
    def GetConnectionAndCursor(self, **kwargs):
        """
        :return: A tuple containing a database connection object and a cursor object.
        :rtype: tuple

        """
        return self.get_connection_and_cursor(**kwargs)

    def _force_connection_closed(self):
        self._cursor.close()
        self._connection.close()
        self._logger.warning("forced connection and cursor to close")
        self._connection, self._cursor = None, None

    def get_connection_and_cursor(self, **kwargs):
        """
        Establishes and retrieves a database connection and its associated cursor object.

        :return: A tuple containing the database connection and the cursor object
        :rtype: tuple
        """
        if self._cursor and self._connection:
            if kwargs.get('force_new', False):
                self._logger.debug("forcing new connection and cursor")
                self._force_connection_closed()
            else:
                self._logger.debug("returning existing connection and cursor")
                return self._connection, self._cursor
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
                raise NoCursorInitializedError()
            except NoCursorInitializedError as e:
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

    def _fetch_results(self):
        try:
            res = self._cursor.fetchall()
        except Exception as e:
            self._logger.debug(e, exc_info=True)
            res = []
        return res

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

            res = self._fetch_results()

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
                raise NoResultsToConvertError()
        if len(final_list_dict) > 0:
            # this returns a sorted list dict instead of an unsorted list dict
            return [dict(sorted(x.items())) for x in final_list_dict]
        return None


class BaseConnectionAttributes(BaseSQLHelper):
    """
    A base class for managing database connection attributes, constructing connection strings,
    and providing mechanisms to populate class attributes either through explicit arguments
    or a provided connection string.

    Constants:
    - _TRUSTED_CONNECTION_DEFAULT: Default value for trusted connection, set to 'yes'.
    - _DRIVER_DEFAULT: Default driver, set to None.
    - _INSTANCE_DEFAULT: Default instance, set to 'SQLEXPRESS'.

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
    _TRUSTED_CONNECTION_DEFAULT = 'yes'
    _DRIVER_DEFAULT = None
    _INSTANCE_DEFAULT = 'SQLEXPRESS'

    def __init__(self, server, database, instance=_INSTANCE_DEFAULT, driver=_DRIVER_DEFAULT,
                 trusted_connection=_TRUSTED_CONNECTION_DEFAULT, **kwargs):
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
                               f"{', '.join(['='.join(x) for x in self.connection_information.items() if x[1] is not None])}")
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
                'username': self.username or '',
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


# noinspection PyUnresolvedReferences
class BaseCreateTriggers(_SharedLogger):
    """
        Class for managing SQLite triggers and audit logging.

        This class extends `SQLlite3Helper` to handle the creation and management
        of database triggers that log changes (inserts, updates, and deletes) made
        on specific tables into an audit log table.

        Methods:
        --------
        __init__(db_file_path: Union[str, Path]):
            Initializes the SQLite connection and ensures the audit log table is created.

        __init_subclass__(**kwargs):
            Ensures that all subclasses define tables to track changes for.

        _create_audit_log_table():
            Creates the audit log table in the SQLite database.

        has_tracked_tables() -> bool:
            Class method to check if any tables have been listed for tracking.

        has_audit_log_table() -> bool:
            Property to check if the audit log table exists in the SQLite database.

        _has_trigger(table: str) -> bool:
            Checks whether audit triggers already exist for a given table.

        _get_column_names(table: str) -> list:
            Retrieves the names of all columns for a given table.

        _get_row_json(columns: list) -> tuple:
            Generates JSON object strings for representing old and new rows
            based on the provided column names.

        create_triggers_for_table(table_name: str, columns: list, commit_triggers: bool=False):
            Creates the INSERT, UPDATE, and DELETE triggers for a given table and optionally commits them.

        generate_triggers_for_all_tables():
            Generates triggers for all the tables in `TABLES_TO_TRACK` if they do
            not already exist and commits the changes to the database.
    """

    _MAGIC_IGNORE_STRING = 'not a value'

    def __init__(self, **kwargs):
        self._cursor = None
        self._connection = None
        self._logger = self._setup_logger()
        self.audit_log_table_init()
        if self.has_required_class_attributes:
            pass

    def __init_subclass__(cls, **kwargs):
        """
        Called when a class is subclassed. Ensures that the subclass has valid configurations regarding table tracking.

        :param kwargs: Additional keyword arguments passed to the subclass.
        :type kwargs: dict
        :raises NoTrackedTablesError: If the subclass is missing tracked tables configuration and is not a table tracker class.
        :return: None
        :rtype: None
        """
        super().__init_subclass__(**kwargs)
        is_missing_tracked_tables = (hasattr(cls, 'TABLES_TO_TRACK')
                                     and cls.TABLES_TO_TRACK == [cls._MAGIC_IGNORE_STRING]
                                     and not cls.is_table_tracker_class())

        if is_missing_tracked_tables:
            raise NoTrackedTablesError()
        # if (not cls.has_tracked_tables()
        #         and not cls.is_table_tracker_class()):
        #     raise NoTrackedTablesError()

    def audit_log_table_init(self):
        """
        Initializes the audit log table by ensuring its existence.

        Calls the `get_connection_and_cursor` method of the class, if available, to establish a database connection. Verifies if the audit log table exists and creates it if it does not. Logs information if the audit log table is already detected. Raises an error if the class does not have the `get_connection_and_cursor` method.

        :raise AttributeError: If the class does not have the method `get_connection_and_cursor`.
        """
        if (hasattr(self, 'get_connection_and_cursor')
                and (not self._cursor or not self._connection)):
            self._logger.info('attempting to connect and get cursor for audit_logging')
            self.get_connection_and_cursor()
        else:
            raise AttributeError("improper subclassing, "
                                 "\'get_connection_and_cursor\' method is missing.")
        if not self.has_audit_log_table:
            self._create_audit_log_table()
        else:
            self._logger.info("audit_log_table detected.")

    @abstractmethod
    def _connect(self):
        """
        Establishes a connection to a specific resource or service.

        This method must be implemented by subclasses to define how the connection
        should be established. It serves as a template for specifying connection
        details and behavior.

        :return: None
        :rtype: NoneType
        """
        ...

    @abstractmethod
    def Query(self, sql_string: str, **kwargs):
        """
        :param sql_string: The SQL query string to be executed.
        :type sql_string: str
        :param kwargs: Additional keyword arguments for executing the query.
        :type kwargs: dict
        :return: The result of the query execution.
        :rtype: Any
        """
        ...

    @property
    @abstractmethod
    def query_results(self):
        """
        Indicates the property `query_results` is an abstract method that must be implemented by any subclass.

        :return: The results of a specific query.
        :rtype: Depends on the implementation in the subclass.
        """
        ...

    @abstractmethod
    def GetConnectionAndCursor(self, **kwargs):
        """
        Fetches a new database connection and associated cursor.

        :return: A tuple containing the database connection and cursor.
        :rtype: tuple
        """
        ...

    @abstractmethod
    def get_connection_and_cursor(self, **kwargs):
        """
        Return a connection object and a cursor object for database interaction.

        :return: A tuple containing the connection object and the cursor object.
        :rtype: tuple
        """
        ...

    @classmethod
    def is_table_tracker_class(cls):
        """
        :return: Indicates whether the class name adheres to the naming convention of starting with an underscore ('_') and ending with 'TableTracker'.
        :rtype: bool

        """
        return (cls.__name__.startswith('_')
                and cls.__name__.endswith('TableTracker'))

    @property
    def has_required_class_attributes(self):
        """
        Checks if all required class attributes are set and not None.

        The method iterates through a list of required class attributes defined in `self.required_class_attributes`
        and verifies if each attribute exists in the current instance and is not None. If all required attributes are
        present and valid, it logs a debug message indicating their status and returns True. Otherwise, it raises
        an exception `MissingRequiredClassAttribute`.

        :return: True if all required class attributes are set and not None
        :rtype: bool
        :raises MissingRequiredClassAttribute: If one or more required class attributes are missing or None
        """
        class_attr = [(hasattr(self, x) and getattr(self, x) is not None)
                      for x in self.required_class_attributes]

        if all(class_attr):
            self._logger.debug(f"All {len(self.required_class_attributes)} "
                               f"required class attributes are set.")
            return True
        raise MissingRequiredClassAttribute()

    @property
    def required_class_attributes(self):
        """
        :return: A list of all uppercase class attribute names.
        :rtype: list
        """
        return [x for x in self.__dir__() if x.isupper()]

    @property
    def class_attr_list(self):
        """
        :return: A dictionary of all non-callable, non-magic attributes of the class.
        :rtype: dict
        """
        class_attrs = {key: value for key, value in self.__class__.__dict__.items() if
                       not callable(value) and not key.startswith("__")}
        return class_attrs

    def _create_audit_log_table(self):
        """
        Creates the audit log table in the database.

        :return: None
        :rtype: None
        """
        self._cursor.execute(self.__class__.AUDIT_LOG_CREATE_TABLE)
        self._connection.commit()
        self._logger.info("Audit log table created.")

    @classmethod
    def has_tracked_tables(cls):
        """
        Checks if there are any tracked tables defined in the TABLES_TO_TRACK attribute.

        :return: True if there are tables to track, False otherwise
        :rtype: bool

        """
        return bool(cls.TABLES_TO_TRACK) if hasattr(cls, 'TABLES_TO_TRACK') else False

    @property
    def has_audit_log_table(self):
        """
        Checks if the audit log table exists by executing a predefined query.

        :return: True if the audit log table exists, False otherwise
        :rtype: bool
        """
        self.Query(self.__class__.AUDIT_LOG_CREATED_CHECK)
        if self.query_results:
            return True
        return False

    def _has_trigger(self, table):
        """
        :param table: The name of the table to check for associated triggers.
        :type table: str
        :return: Returns True if the table has associated triggers, otherwise False.
        :rtype: bool
        """
        self.Query(self.__class__.HAS_TRIGGER_CHECK.format(table=table))
        if self.query_results:
            return True
        return False

    def _get_column_names(self, table):
        self.Query(self.__class__.GET_COLUMN_NAMES.format(table=table))
        if self.query_results:
            return [x[0] for x in self.query_results]

    @staticmethod
    def _get_row_json(columns):
        """
        :param columns: List of column names to be used for generating JSON objects.
        :type columns: list of str
        :return: A tuple containing two strings, `new_row_json` and `old_row_json`.
                 Each string is a JSON object representation using the given columns.
        :rtype: tuple
        """
        # changed to .format instead of f-strings to preserve backwards compatibility with py <=3.8
        new_row_json = "json_object({})".format(
            ', '.join(["'{}', NEW.{}".format(col, col) for col in columns])
        )
        old_row_json = "json_object({})".format(
            ', '.join(["'{}', OLD.{}".format(col, col) for col in columns])
        )
        return new_row_json, old_row_json

    def create_triggers_for_table(self, table_name, columns, commit_triggers=False):
        """
        :param table_name: Name of the database table for which triggers are to be created.
        :type table_name: str
        :param columns: List of column names to be included in the triggers.
        :type columns: list
        :param commit_triggers: Flag indicating whether the changes should be committed to the database. Defaults to False.
        :type commit_triggers: bool
        :return: None
        :rtype: None
        """
        new_row_json, old_row_json = self._get_row_json(columns)

        # INSERT Trigger for table_name
        insert_trigger_query = self.__class__.INSERT_TRIGGER.format(table_name=table_name,
                                                                    new_row_json=new_row_json)
        self._cursor.execute(insert_trigger_query)

        # UPDATE Trigger for table_name
        update_trigger_query = self.__class__.UPDATE_TRIGGER.format(table_name=table_name,
                                                                    old_row_json=old_row_json,
                                                                    new_row_json=new_row_json)
        self._cursor.execute(update_trigger_query)

        # DELETE Trigger for table_name
        delete_trigger_query = self.__class__.DELETE_TRIGGER.format(table_name=table_name,
                                                                    old_row_json=old_row_json)
        self._cursor.execute(delete_trigger_query)

        if not commit_triggers:
            self._logger.warning(f"triggers for {table_name} created but NOT COMMITTED.")
        else:
            self._connection.commit()
            self._logger.info(f"triggers for {table_name} created and committed.")

    def generate_triggers_for_all_tables(self):
        """
        Generates database triggers for all the tables listed in `TABLES_TO_TRACK`.

        This function iterates through each table in `TABLES_TO_TRACK` and checks if
        the table already has triggers. If triggers are not present for a table, it
        creates them by calling `create_triggers_for_table` using the table name as
        well as the column names retrieved from `_get_column_names`. Debug and
        informational logging is performed during this process to record trigger
        generation status for each table. After successfully generating triggers for
        all tables, the changes are committed to the database.

        :return: None
        :rtype: None
        """
        self._logger.info(f"Attempting to generate triggers for {len(self.__class__.TABLES_TO_TRACK)} tables")
        trigger_create_counter = 0
        already_created_counter = 0

        for table in self.__class__.TABLES_TO_TRACK:
            if not self._has_trigger(table):
                trigger_create_counter = + 1
                self.create_triggers_for_table(table, self._get_column_names(table))
                self._logger.debug(f'triggers for {table} created')
                print(f'triggers for {table} created')
            else:
                already_created_counter = + 1
                print(f'{table} already has triggers')
                self._logger.debug(f'{table} already has triggers')

        if trigger_create_counter > 0:
            self._logger.info(f'{trigger_create_counter} trigger(s) generated successfully')

            self._logger.info('committing triggers')
            self._connection.commit()
            self._logger.info('triggers committed successfully')
        if already_created_counter > 0:
            self._logger.info(f'{already_created_counter} trigger(s) were already present')
