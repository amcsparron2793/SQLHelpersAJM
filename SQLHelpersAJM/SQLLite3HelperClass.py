import sqlite3
from abc import abstractmethod
from typing import Union
from pathlib import Path
from SQLHelpersAJM import BaseSQLHelper, BaseCreateTriggers
from backend import ABCCreateTriggers


class _SQLlite3TableTracker(BaseCreateTriggers):
    TABLES_TO_TRACK = [BaseCreateTriggers._MAGIC_IGNORE_STRING]
    AUDIT_LOG_CREATE_TABLE = """create table audit_log
                                        (
                                            id           INTEGER
                                                primary key autoincrement,
                                            table_name   TEXT not null,
                                            operation    TEXT not null,
                                            old_row_data TEXT,
                                            new_row_data TEXT,
                                            change_time  TIMESTAMP default CURRENT_TIMESTAMP
                                        );"""
    AUDIT_LOG_CREATED_CHECK = "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_log';"
    HAS_TRIGGER_CHECK = """select tbl_name 
                                from sqlite_master 
                                where type='trigger' 
                                    and tbl_name='{table}';"""
    GET_COLUMN_NAMES = """SELECT p.name as columnName
                                FROM sqlite_master m
                                left outer join pragma_table_info((m.name)) p
                                    on m.name <> p.name
                                where m.name = '{table}';"""
    INSERT_TRIGGER = """
                    CREATE TRIGGER after_{table_name}_insert
                    AFTER INSERT ON {table_name}
                    BEGIN
                        INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                        VALUES (
                            '{table_name}', 
                            'INSERT', 
                            NULL, 
                            {new_row_json}
                        );
                    END;
                    """

    UPDATE_TRIGGER = """
                    CREATE TRIGGER after_{table_name}_update
                    AFTER UPDATE ON {table_name}
                    BEGIN
                        INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                        VALUES (
                            '{table_name}', 
                            'UPDATE', 
                            {old_row_json}, 
                            {new_row_json}
                        );
                    END;
                    """

    DELETE_TRIGGER = """
                CREATE TRIGGER after_{table_name}_delete
                AFTER DELETE ON {table_name}
                BEGIN
                    INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                    VALUES (
                        '{table_name}', 
                        'DELETE', 
                        {old_row_json}, 
                        NULL
                    );
                END;
                """

    @abstractmethod
    def _connect(self):
        ...


class SQLlite3Helper(BaseSQLHelper):
    """ Initializes an SQLlite3 database and has a basic query method.
    This class is meant to be subclassed and expanded.

    IF NO LOGGER IS SPECIFIED, A DUMMY LOGGER IS USED. """

    def __init__(self, db_file_path: Union[str, Path], **kwargs):
        self.logger_level = kwargs.get('logger_level', 'INFO')
        self.db_file_path = db_file_path
        super().__init__(**kwargs)

    def _setup_logger(self, **kwargs):
        return super()._setup_logger(basic_config_level=self.logger_level)

    @property
    def __version__(self):
        return "1.3.0"

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


class SQLite3HelperTT(SQLlite3Helper, _SQLlite3TableTracker, metaclass=ABCCreateTriggers):
    TABLES_TO_TRACK = ['test_table'],

    def __init__(self, db_file_path: Union[str, Path], **kwargs):
        super().__init__(db_file_path, **kwargs)
        _SQLlite3TableTracker.__init__(self, **kwargs)

    @property
    def __version__(self):
        return "0.0.1"


if __name__ == "__main__":
    junk_db_filepath = r"C:\Users\amcsparron\Desktop\Python_Projects\SQLHelpersAJM\Misc_Project_Files\test_db.db"
    # sql = SQLlite3Helper(db_file_path=junk_db_filepath)
    sql_tt = SQLite3HelperTT(db_file_path=junk_db_filepath)
    # sql_tt.query("insert into test_table(name, age) VALUES ('andrew', 32) returning id;", is_commit=True)
    # sql_tt.query("select * from audit_log;", is_commit=False)
    #print(sql_tt.query_results)
    #print(sql.class_attr_list)
    #print(sql.required_class_attributes)
    #sql.get_connection_and_cursor()
    #sql.query("drop table test_table;", is_commit=True)
    #sql_tt.query("create table test_table (id integer primary key autoincrement, name varchar(255), age integer);", is_commit=True)
    #sql.query("select * from test_table;", is_commit=False)
    #print(sql.query_results)
