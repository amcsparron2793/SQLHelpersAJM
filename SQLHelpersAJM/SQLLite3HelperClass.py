import sqlite3
from logging import Logger, getLogger
from typing import Union
from pathlib import Path
from SQLHelpersAJM import _BaseSQLHelper


class SQLlite3Helper(_BaseSQLHelper):
    """ Initializes an SQLlite3 database and has a basic query method.
    This class is meant to be subclassed and expanded.

    IF NO LOGGER IS SPECIFIED, A DUMMY LOGGER IS USED. """
    def __init__(self, db_file_path: Union[str, Path], logger: Logger = None, **kwargs):
        if logger:
            self._logger = logger
        else:
            self._logger = getLogger(__name__)
        super().__init__(logger=self._logger, **kwargs)
        self.db_file_path = db_file_path

    def _connect(self):
        self._logger.info(f"Attempting  to connect to {self.db_file_path}")
        self._connection = sqlite3.connect(self.db_file_path)

        # print("Connection was successful")
        self._logger.info("Connection was successful")
        return self._connection

    def _set_foreign_keys_on(self):
        self._cursor.execute("PRAGMA foreign_keys = ON;")
        self._logger.debug("PRAGMA foreign_keys set to ON")
        self._connection.commit()

    def get_connection_and_cursor(self):
        self._connection, self._cursor = super().get_connection_and_cursor()
        self._set_foreign_keys_on()
        return self._connection, self._cursor


if __name__ == "__main__":
    sql = SQLlite3Helper(db_file_path=r"C:\Users\amcsparron\Desktop\Python_Projects\SQLHelpersAJM\Misc_Project_Files\test_db.db")
    #sql.get_connection_and_cursor()
    #sql.query("drop table test_table;", is_commit=True)
    #sql.query("create table test_table (id integer primary key autoincrement, name varchar(255), age integer);", is_commit=True)
    #sql.query("insert into test_table(name, age) VALUES ('andrew', 32) returning id;", is_commit=True)
    sql.query("select * from test_table;", is_commit=False)
    print(sql.query_results)