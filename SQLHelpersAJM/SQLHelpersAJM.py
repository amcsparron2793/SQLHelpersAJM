"""
SQLHelpersAJM.py

classes meant to streamline interaction with multiple different flavors of SQL database including MSSQL and SQLlite

"""
from abc import abstractmethod
from collections import ChainMap
from logging import basicConfig
from typing import Optional, List, Union
import logging

from _backend import deprecated, _NoCursorInitializedError, _NoResultsToConvertError
from _version import __version__


class _BaseSQLHelper:
    def __init__(self, **kwargs):
        self._logger = kwargs.get('logger', logging.getLogger(__name__))
        if self._logger.hasHandlers():
            pass
        else:
            basicConfig(level='INFO')
        self._connection_string = kwargs.get('connection_string', None)
        self._connection, self._cursor = None, None
        self._query_results = None
        if self._logger:
            self._logger.info(f"initialized {self.__class__.__name__} v{__version__}")

    @property
    def is_ready_for_query(self):
        """
        Determines if the instance is ready to execute a query.

        :return: True if the cursor object exists, otherwise False
        :rtype: bool
        """
        if self._cursor:
            return True
        return False

    @abstractmethod
    def _connect(self):
        """
        Establishes a connection to a database using the specified connection string.

        :return: A connection object if the connection is successful.
        :rtype: Any
        """
        ...

    def log_and_raise_error(self, err: Exception):
        self._logger.error(err, exc_info=True)
        raise err from None

    @deprecated(
        "This method is deprecated and will be removed in a future release. "
        "Please use the get_connection_and_cursor method instead.")
    def GetConnectionAndCursor(self):
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
                raise e

    @deprecated(
        "This method is deprecated and will be removed in a future release. "
        "Please use the query method instead.")
    def Query(self, sql_string: str, **kwargs):
        self.query(sql_string, **kwargs)

    @staticmethod
    def normalize_single_result(result) -> (Union[Optional[tuple], Optional[list], Optional[dict], Optional[str], Optional[int]]):
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
                    self._logger.warning(f"query returned no results")
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
        else:
            return None

    @property
    def results_column_names(self) -> List[str] or None:
        """
        :return: A list of column names of the results from the cursor description, or None if the cursor description is not available.
        :rtype: List[str] or None
        """
        try:
            return [d[0] for d in self._cursor.description]
        except AttributeError as e:
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
        else:
            return None
