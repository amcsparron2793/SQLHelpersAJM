import sqlite3
from typing import Union
from pathlib import Path
from SQLHelpersAJM import _BaseSQLHelper, _BaseCreateTriggers

class SQLlite3TableTracker(_BaseCreateTriggers):
    TABLES_TO_TRACK = ['askjd']
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.has_required_class_attributes:
            pass


class SQLlite3Helper(SQLlite3TableTracker):
    """ Initializes an SQLlite3 database and has a basic query method.
    This class is meant to be subclassed and expanded.

    IF NO LOGGER IS SPECIFIED, A DUMMY LOGGER IS USED. """

    def __init__(self, db_file_path: Union[str, Path], **kwargs):
        self.db_file_path = db_file_path
        super().__init__(**kwargs)
        # FIXME: this needs to be able to check against its TableTracker class attributes NOT only local,
        #  but not further back than its immediate parent

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


if __name__ == "__main__":
    sql = SQLlite3Helper(db_file_path=r"C:\Users\amcsparron\Desktop\Python_Projects\SQLHelpersAJM\Misc_Project_Files\test_db.db")
    print(sql.class_attr_list)
    print(sql.required_class_attributes)
    #sql.get_connection_and_cursor()
    #sql.query("drop table test_table;", is_commit=True)
    #sql.query("create table test_table (id integer primary key autoincrement, name varchar(255), age integer);", is_commit=True)
    #sql.query("insert into test_table(name, age) VALUES ('andrew', 32) returning id;", is_commit=True)
    #sql.query("select * from test_table;", is_commit=False)
    #print(sql.query_results)
