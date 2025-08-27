# pylint: disable=line-too-long
# pylint: disable=import-error
from abc import abstractmethod

import pyodbc
from SQLHelpersAJM.helpers.bases import BaseConnectionAttributes, BaseCreateTriggers
from SQLHelpersAJM.backend.errors import NoTrackedTablesError
from SQLHelpersAJM.backend.meta import ABCCreateTriggers


# noinspection SqlResolve,SqlIdentifier
class _SQLServerTableTracker(BaseCreateTriggers):
    """
    _Internal class to handle SQL Server table tracking by creating and managing audit triggers._

    This class creates triggers on specified tables to track changes (insert, update, delete) and logs them into an `audit_log` table. It provides predefined SQL statements for managing triggers and retrieving metadata information necessary for creating and ensuring the audit mechanism remains functional.

    Attributes:
        TABLES_TO_TRACK: List of tables for which triggers need to be created. Defaults to a placeholder value.
        AUDIT_LOG_CREATE_TABLE: SQL query string to create the `audit_log` table if it does not exist.
        AUDIT_LOG_CREATED_CHECK: SQL query string to verify the existence of the `audit_log` table.
        HAS_TRIGGER_CHECK: SQL query string to check if a specific table already has associated triggers.
        GET_COLUMN_NAMES: SQL query string to fetch column names of a specific table.
        INSERT_TRIGGER: SQL query string to create a trigger that logs insert operations into the `audit_log` table.
        UPDATE_TRIGGER: SQL query string to create a trigger that logs update operations into the `audit_log` table.
        DELETE_TRIGGER: SQL query string to create a trigger that logs delete operations into the `audit_log` table.
    """
    TABLES_TO_TRACK = [BaseCreateTriggers._MAGIC_IGNORE_STRING]
    AUDIT_LOG_CREATE_TABLE = """CREATE TABLE audit_log
(
    id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(255) NOT NULL,
    operation NVARCHAR(50) NOT NULL,
    old_row_data NVARCHAR(MAX),
    new_row_data NVARCHAR(MAX),
    change_time DATETIME DEFAULT CURRENT_TIMESTAMP
);"""
    AUDIT_LOG_CREATED_CHECK = """SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'audit_log';"""
    HAS_TRIGGER_CHECK = """SELECT name 
FROM sys.triggers 
WHERE parent_id = OBJECT_ID('{table}');"""
    GET_COLUMN_NAMES = """SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = '{table}';"""

    INSERT_TRIGGER = """CREATE TRIGGER after_{table_name}_insert
                        ON {table_name}
                        AFTER INSERT
                        AS
                        BEGIN
                            INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                            SELECT 
                                '{table_name}' AS table_name, 
                                'INSERT' AS operation, 
                                NULL AS old_row_data, 
                                (SELECT * FROM INSERTED FOR JSON AUTO) AS new_row_json
                            FROM INSERTED;
                        END;"""

    UPDATE_TRIGGER = """CREATE TRIGGER after_{table_name}_update
                        ON {table_name}
                        AFTER UPDATE
                        AS
                        BEGIN
                            INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                            SELECT 
                                '{table_name}' AS table_name, 
                                'UPDATE' AS operation,  
                                (SELECT * FROM DELETED FOR JSON AUTO) AS old_row_json, 
                                (SELECT * FROM INSERTED FOR JSON AUTO) AS new_row_json
                            FROM INSERTED 
                            INNER JOIN DELETED 
                            ON INSERTED.id = DELETED.id;
                        END;"""
    # noinspection SqlWithoutWhere
    DELETE_TRIGGER = """CREATE TRIGGER after_{table_name}_delete
                        ON {table_name}
                        AFTER DELETE
                        AS
                        BEGIN
                            INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                            SELECT 
                                '{table_name}' AS table_name, 
                                'DELETE' AS operation,  
                                (SELECT * FROM DELETED FOR JSON AUTO) AS old_row_json, 
                                NULL AS new_row_json
                            FROM DELETED;
                        END;"""

    _GET_TRIGGER_INFO = """SELECT
                            t.name AS TriggerName,
                            t.is_disabled AS IsDisabled,
                            s.name AS SchemaName,
                            o.name AS TableName,
                            o.type_desc AS ObjectType,
                            t.create_date AS CreatedDate,
                            t.modify_date AS LastModifiedDate
                        FROM
                            sys.triggers AS t
                        JOIN
                            sys.objects AS o ON t.parent_id = o.object_id
                        JOIN
                            sys.schemas AS s ON o.schema_id = s.schema_id
                        WHERE
                            t.type_desc = 'SQL_TRIGGER'
                        ORDER BY
                            t.name;"""

    @abstractmethod
    def _connect(self):
        ...


class SQLServerHelper(BaseConnectionAttributes):
    """
    This class provides methods and attributes to facilitate interactions with a SQL Server database.

    It inherits from `BaseConnectionAttributes`, and it is used for establishing and managing
    database connections by leveraging the pyodbc library.
    """
    _DRIVER_DEFAULT = '{SQL Server}'
    _TRUSTED_CONNECTION_DEFAULT = 'yes'
    _INSTANCE_DEFAULT = 'SQLEXPRESS'
    _DEFAULT_PORT = 1433

    def __init__(self, server, database, **kwargs):
        self.server = server
        self.database = database
        self._logger = self._setup_logger(**kwargs)
        super().__init__(self.server, self.database, **kwargs)

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
        return '0.1'


class SQLServerHelperTT(SQLServerHelper, _SQLServerTableTracker, metaclass=ABCCreateTriggers):
    """
    SQLServerHelperTT is a specialized class that combines the functionality of SQLServerHelper and _SQLServerTableTracker,
    with a custom metaclass ABCCreateTriggers applied to provide automatic trigger creation for table tracking in a SQL Server.

    Attributes:
        TABLES_TO_TRACK: A list to specify the tables to be tracked by this class.

    Methods:
        __init__: Initializes the SQLServerHelperTT object, calling the constructors of SQLServerHelper and _SQLServerTableTracker.
        __version__: A property that returns the current version of the class.
    """

    def __init__(self, server, database, **kwargs):
        super().__init__(server, database, **kwargs)
        _SQLServerTableTracker.__init__(self, **kwargs)

    def __new__(cls, *args, **kwargs):
        if cls.TABLES_TO_TRACK == [cls._MAGIC_IGNORE_STRING]:
            raise NoTrackedTablesError(class_name=cls.__name__)
        return super().__new__(cls)

    @property
    def __version__(self):
        return "0.1"


if __name__ == '__main__':
    # noinspection SpellCheckingInspection
    gis_prod_connection_string = ("server=10NE-WTR44;trusted_connection=yes;"
                                  f"database=gisprod;username=sa;password={None}")
    SQLServerHelperTT.TABLES_TO_TRACK = ['gisprod']
    sql_srv = SQLServerHelperTT.with_connection_string(gis_prod_connection_string)#, basic_config_level='DEBUG')
    print(sql_srv)
    sql_srv.get_all_trigger_info(print_info=True)
