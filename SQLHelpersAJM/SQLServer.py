# pylint: disable=line-too-long
# pylint: disable=import-error

import pyodbc
from SQLHelpersAJM import _BaseConnectionAttributes


class SQLServerHelper(_BaseConnectionAttributes):
    """
    A helper class to interact with an SQL Server database using pyodbc.
    Provides utility methods for connection management, executing queries, and formatting query results.

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

        with_connection_string(cls, connection_string: Optional[str],
                                attr_split_char: str = ';', key_value_split_char: str = '=', **kwargs):
            Initializes a class instance by extracting attributes from a provided connection string.
            Raises an error if the connection string is not provided.

            :param connection_string: A string containing the connection attributes separated by attr_split_char
                and key-value pairs separated by key_value_split_char. This parameter is mandatory and cannot be None.
            :type connection_string: Optional[str]
            :param attr_split_char: The character used to split the connection attributes in the connection_string.
                Default is ';'.
            :type attr_split_char: str
            :param key_value_split_char: The character used to separate keys and values in
                each connection attribute in the connection_string. Default is '='.
            :type key_value_split_char: str
            :param kwargs: Additional keyword arguments to be passed during the initialization of the class.
            :return: An instance of the class initialized with the attributes parsed from the connection_string
                and additional keyword arguments.
            :rtype: cls

        cursor_check(self):
            Checks if the cursor is properly initialized and ready for executing queries.
            If the cursor is not initialized, it raises a `NoCursorInitializedError`, logs the error, and rethrows it.

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

            :return: A list of dictionaries obtained from the processed query results,
                or None if no query results are available.
            :rtype: list[dict] or None

        results_column_names(self) -> List[str] or None:
            Retrieves the column names of the query results if available.

            :return: A list of column names or None if the cursor description is not available.
            :rtype: List[str] or None

        _ConvertToFinalListDict(self, results: List[tuple]) -> List[dict] or None:
            Converts a list of tuples representing query results into a list of dictionaries.
                Maps each tuple's values to its corresponding column names.

            :param results: A list of tuples where each tuple represents a row of data.
            :type results: List[tuple]
            :return: A sorted list of dictionaries, where each dictionary corresponds to a row of data,
                or None if no valid data exists.
            :rtype: List[dict] or None
    """
    DRIVER_DEFAULT = '{SQL Server}'

    def _connect(self):
        """
        Establishes a connection to a database using the specified connection string.

        :return: A connection object if the connection is successful.
        :rtype: pyodbc.Connection
        :raises pyodbc.Error: If there is an error while attempting to connect to the database.
        """
        cxn = pyodbc.connect(self.connection_string)
        self._logger.debug("connection successful")
        self._password = 'NONE'
        return cxn

    @property
    def __version__(self):
        return '0.0.1'


if __name__ == '__main__':
    gis_prod_connection_string = ("driver={SQL Server};server=10NE-WTR44;instance=SQLEXPRESS;"
                                  "database=gisprod;"
                                  "trusted_connection=no;username=sa;password=A1bany2025!")
    #SQLServerHelper.with_connection_string(gis_prod_connection_string)#server='10.56.211.116', database='gisprod')
    #sql_srv = SQLServerHelper(server='10NE-WTR44', instance='SQLEXPRESS', database='gisprod')#, username='sa', password='A1bany2025!')
    sql_srv = SQLServerHelper.with_connection_string(gis_prod_connection_string)
    sql_srv.get_connection_and_cursor()
    sql_srv.query("select SYSTEM_USER")
    print(sql_srv.query_results)
