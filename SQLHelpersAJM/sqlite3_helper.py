import sqlite3
from abc import abstractmethod
from typing import Union
from pathlib import Path
from SQLHelpersAJM import BaseSQLHelper, BaseCreateTriggers
from backend import ABCCreateTriggers, NoTrackedTablesError


class _SQLite3TableTracker(BaseCreateTriggers):
    """
    This class, `_SQLite3TableTracker`, extends the `BaseCreateTriggers` to provide functionality for tracking changes in SQLite tables via triggers and an audit log.

    Attributes:
        TABLES_TO_TRACK: A list of table names that will be tracked for changes. This includes a placeholder string `_MAGIC_IGNORE_STRING` defined in the `BaseCreateTriggers` class, which likely serves a specific purpose in the base class's implementation.
        AUDIT_LOG_CREATE_TABLE: The SQL statement used to create the `audit_log` table, which stores audit records of changes within the tracked tables. Columns include:
            - `id`: Primary key.
            - `table_name`: Name of the table where the change occurred.
            - `operation`: Type of operation ("INSERT", "UPDATE", or "DELETE").
            - `old_row_data`: JSON representation of the row data before the change (if applicable).
            - `new_row_data`: JSON representation of the row data after the change (if applicable).
            - `change_time`: Timestamp of the change (default is the current timestamp).
        AUDIT_LOG_CREATED_CHECK: SQL query to verify whether the `audit_log` table exists in the SQLite schema.
        HAS_TRIGGER_CHECK: SQL query to determine if triggers are already associated with a particular table. Replaces `{table}` with the specific table's name to check.
        GET_COLUMN_NAMES: SQL query to obtain the column names for a given table. Replaces `{table}` with the specific table's name.
        INSERT_TRIGGER: SQL statement that defines an "AFTER INSERT" trigger for a specified table. This trigger logs the new row's data into the `audit_log`.
        UPDATE_TRIGGER: SQL statement that defines an "AFTER UPDATE" trigger for a specified table. Logs both the old and new data of the affected row into the `audit_log`.
        DELETE_TRIGGER: SQL statement that defines an "AFTER DELETE" trigger for a specified table. Logs the old data of the deleted row into the `audit_log`.

    Methods:
        _connect: Abstract method to be implemented by subclasses. It is expected to establish and return a connection to the SQLite database.
    """
    # MAGIC IGNORE STRING is used so that a type error is not thrown for an undefined attribute
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

    _GET_TRIGGER_INFO = """SELECT
                            name AS TriggerName,
                            tbl_name AS TableName,
                            sql AS TriggerDefinition
                        FROM 
                            sqlite_master
                        WHERE 
                            type = 'trigger'
                        ORDER BY 
                            name;"""

    @abstractmethod
    def _connect(self):
        ...


class SQLite3Helper(BaseSQLHelper):
    """
    SQLite3Helper class that provides utility functions for managing SQLite3 database connections, logging, and enforcing foreign key constraints.

    Superclass:
        BaseSQLHelper: The base class containing shared functionality for SQL helpers.

    Methods:
        __init__(db_file_path, **kwargs):
            Initializes the SQLite3Helper instance with the provided database file path and optional kwargs for configuration settings, such as logger level.

        _setup_logger(**kwargs):
            Configures and returns a logger with the given settings. Defaults the basic logging configuration level to the logger_level specified during initialization.

        __version__:
            A property that returns the version of the SQLite3Helper class.

        _connect():
            Establishes a connection to the SQLite3 database file specified during initialization. Logs the success or failure of the connection.

        _set_foreign_keys_on():
            Enables foreign key constraints for the SQLite3 database connection. Enforces data integrity rules set by the foreign keys.

        get_connection_and_cursor():
            Retrieves the SQLite3 database connection and cursor, ensures foreign key constraints are enabled, and returns the connection and cursor.
    """

    def __init__(self, db_file_path: Union[str, Path], **kwargs):
        self.db_file_path = db_file_path
        super().__init__(**kwargs)

    @property
    def __version__(self):
        """
        :return: Returns the current version of the application or module.
        :rtype: str
        """
        return "1.3.0"

    def _connect(self):
        """
        Establishes a connection to the SQLite database specified by the `db_file_path`.
        Logs the connection attempt and its success.

        :return: SQLite database connection object
        :rtype: sqlite3.Connection

        """
        self._logger.info(f"Attempting  to connect to {self.db_file_path}")
        self._connection = sqlite3.connect(self.db_file_path)

        # print("Connection was successful")
        self._logger.info("Connection was successful")
        return self._connection

    def _set_foreign_keys_on(self):
        """
        Enables foreign key constraint for the SQLite database by setting "PRAGMA foreign_keys" to ON.
        Commits the change to the database and logs the operation.

        :return: None
        :rtype: None
        """
        self._cursor.execute("PRAGMA foreign_keys = ON;")
        self._logger.debug("PRAGMA foreign_keys set to ON")
        self._connection.commit()

    def get_connection_and_cursor(self, **kwargs):
        """
        Establishes a database connection and retrieves a cursor. Ensures that foreign key constraints are enforced by calling a specific method to activate them.

        :return: A tuple containing the database connection object and cursor.
        :rtype: tuple
        """
        self._connection, self._cursor = super().get_connection_and_cursor(**kwargs)
        self._set_foreign_keys_on()
        return self._connection, self._cursor


class SQLite3HelperTT(SQLite3Helper, _SQLite3TableTracker, metaclass=ABCCreateTriggers):
    """
    SQLite3HelperTT is a specialized class that combines functionalities from SQLite3Helper and
    _SQLite3TableTracker while using the ABCCreateTriggers metaclass. The class is designed
    to provide enhanced management of SQLite3 databases, including table tracking and trigger
    management, for the specified TABLES_TO_TRACK.

    Attributes:
        TABLES_TO_TRACK:
            A list of tables to be tracked within the SQLite database. This is static and predefined.

    Methods:
        __init__(db_file_path, **kwargs):
            Initializes the SQLite3HelperTT instance. It sets up the class by invoking initializations
            from its parent classes SQLite3Helper and _SQLite3TableTracker. The db_file_path argument
            specifies the path to the SQLite database, and additional keyword arguments can be provided
            for further customization.

    Properties:
        __version__:
            Returns the current version of the SQLite3HelperTT class implementation.
    """

    def __init__(self, db_file_path: Union[str, Path], **kwargs):
        super().__init__(db_file_path, **kwargs)
        _SQLite3TableTracker.__init__(self, **kwargs)

    def __new__(cls, *args, **kwargs):
        if cls.TABLES_TO_TRACK == [cls._MAGIC_IGNORE_STRING]:
            raise NoTrackedTablesError(class_name=cls.__name__)
        return super().__new__(cls)

    @property
    def __version__(self):
        return "0.0.1"


if __name__ == "__main__":
    junk_db_filepath = r"C:\Users\amcsparron\Desktop\Python_Projects\SQLHelpersAJM\Misc_Project_Files\test_db.db"
    # sql = SQLlite3Helper(db_file_path=junk_db_filepath)
    sql_tt = SQLite3HelperTT(db_file_path=junk_db_filepath)
    sql_tt.get_all_trigger_info(print_info=True)
