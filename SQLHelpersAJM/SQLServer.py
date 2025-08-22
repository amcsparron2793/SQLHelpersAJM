# pylint: disable=line-too-long
# pylint: disable=import-error
from abc import abstractmethod

import pyodbc
from SQLHelpersAJM import BaseConnectionAttributes, BaseCreateTriggers
from backend import ABCCreateTriggers


# noinspection SqlResolve,SqlIdentifier
class _SQLServerTableTracker(BaseCreateTriggers):
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

    def __init__(self, server, database, driver=_DRIVER_DEFAULT, **kwargs):
        self.server = server
        self.database = database
        self.driver = driver
        self._logger = self._setup_logger(**kwargs)
        super().__init__(self.server, self.database, driver=self.driver, **kwargs)

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


class SQLServerHelperTT(SQLServerHelper, _SQLServerTableTracker, metaclass=ABCCreateTriggers):
    TABLES_TO_TRACK = ['AndrewTestTable']

    def __init__(self, server, database, **kwargs):
        super().__init__(server, database, **kwargs)
        _SQLServerTableTracker.__init__(self, **kwargs)

    @property
    def __version__(self):
        return "0.0.1"


if __name__ == '__main__':
    # noinspection SpellCheckingInspection
    gis_prod_connection_string = ("driver={SQL Server};server=10NE-WTR44;instance=SQLEXPRESS;"
                                  "database=AndrewTest;"
                                  "trusted_connection=yes;username=sa;password=")
    #SQLServerHelper.with_connection_string(gis_prod_connection_string)#server='10.56.211.116', database='gisprod')
    #sql_srv = SQLServerHelper(server='10NE-WTR44', instance='SQLEXPRESS', database='gisprod')#, username='sa', password=)
    #sql_srv = SQLServerHelper.with_connection_string(gis_prod_connection_string)
    #sql_srv.get_connection_and_cursor()
    sql_srv = SQLServerHelperTT.with_connection_string(gis_prod_connection_string)#, basic_config_level='DEBUG')
    #sql_srv.query("select SYSTEM_USER")
    #sql_srv.query("insert into AndrewTestTable(FirstName, LastName) VALUES ('andrew', 'mcsparron') --returning id;", is_commit=True)
    #sql_srv.generate_triggers_for_all_tables()
#     sql_srv.query("""SELECT
#     t.name AS TriggerName,
#     t.is_disabled AS IsDisabled,
#     s.name AS SchemaName,
#     o.name AS TableName,
#     o.type_desc AS ObjectType,
#     t.create_date AS CreatedDate,
#     t.modify_date AS LastModifiedDate
# FROM
#     sys.triggers AS t
# JOIN
#     sys.objects AS o ON t.parent_id = o.object_id
# JOIN
#     sys.schemas AS s ON o.schema_id = s.schema_id
# WHERE
#     t.type_desc = 'SQL_TRIGGER'
# ORDER BY
#     t.name;""", is_commit=False)
    sql_srv.query("select * from audit_log;", is_commit=False)
    print(sql_srv.query_results)
