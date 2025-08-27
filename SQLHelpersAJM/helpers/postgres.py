from abc import abstractmethod

import psycopg
from SQLHelpersAJM.helpers.bases import BaseConnectionAttributes, BaseCreateTriggers
from SQLHelpersAJM.backend.meta import ABCPostgresCreateTriggers
from SQLHelpersAJM.backend.errors import NoTrackedTablesError


# noinspection SqlNoDataSourceInspection
class _PostgresTableTracker(BaseCreateTriggers):
    """
    Class for tracking PostgreSQL table changes by creating audit log triggers.

    This class extends functionality to automate the process of adding triggers to PostgreSQL tables for audit logging. It ensures that operations such as insert, update, and delete on specified tables are logged into a dedicated `audit_log` table. The class uses PostgreSQL's trigger functionality and PL/pgSQL functions to facilitate this.

    Attributes:
        TABLES_TO_TRACK: List of tables that require change tracking. Default includes ignored markers.

        AUDIT_LOG_CREATE_TABLE: SQL query to create the `audit_log` table, which stores the audit data.
        AUDIT_LOG_CREATED_CHECK: SQL query to check if the `audit_log` table exists.
        HAS_TRIGGER_CHECK: PL/pgSQL block to check whether a trigger exists on a specific table.
        GET_COLUMN_NAMES: SQL query to retrieve the column names of a specific table.

        LOG_AFTER_INSERT_FUNC: PL/pgSQL function that logs `INSERT` operations to the `audit_log` table.
        LOG_AFTER_UPDATE_FUNC: PL/pgSQL function that logs `UPDATE` operations to the `audit_log` table.
        LOG_AFTER_DELETE_FUNC: PL/pgSQL function that logs `DELETE` operations to the `audit_log` table.

        FUNC_EXISTS_CHECK: SQL query to check for the existence of a specific function within a schema.

        INSERT_TRIGGER: SQL query template to create a `AFTER INSERT` trigger for a specified table.
        UPDATE_TRIGGER: SQL query template to create a `AFTER UPDATE` trigger for a specified table.
        DELETE_TRIGGER: SQL query template to create a `AFTER DELETE` trigger for a specified table.

        _GET_TRIGGER_INFO: SQL query to retrieve metadata and details of triggers in the database.

    Methods:
        _connect:
            Abstract method placeholder for defining database connection logic in derived classes.
    """
    TABLES_TO_TRACK = [BaseCreateTriggers._MAGIC_IGNORE_STRING]

    AUDIT_LOG_CREATE_TABLE = """CREATE TABLE audit_log (
                                    id SERIAL PRIMARY KEY,
                                    table_name TEXT NOT NULL,
                                    operation TEXT NOT NULL,
                                    old_row_data JSONB,
                                    new_row_data JSONB,
                                    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                );"""

    AUDIT_LOG_CREATED_CHECK = """ select EXISTS(SELECT FROM pg_tables 
                                    WHERE schemaname = 'public' 
                                    AND tablename = 'audit_log');"""

    HAS_TRIGGER_CHECK = """DO $$
                            DECLARE
                                trigger_found BOOLEAN;
                            BEGIN
                                SELECT EXISTS (
                                    SELECT 1
                                    FROM pg_trigger
                                    WHERE tgname = 'after_' || '{table}' || '_insert'
                                ) INTO trigger_found;
                            
                                IF trigger_found THEN
                                    RAISE NOTICE 'Trigger exists on table {table}.';
                                ELSE
                                    RAISE NOTICE 'Trigger does not exist on table {table}.';
                                END IF;
                            END;
                            $$;"""

    GET_COLUMN_NAMES = """SELECT column_name AS columnName
                            FROM information_schema.columns
                            WHERE table_name = '{table}';"""

    LOG_AFTER_INSERT_FUNC = """CREATE OR REPLACE FUNCTION log_after_insert() RETURNS TRIGGER AS $$
                                BEGIN
                                    INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                                    VALUES (
                                        TG_TABLE_NAME,
                                        'INSERT',
                                        NULL,
                                        row_to_json(NEW)::jsonb
                                    );
                                    RETURN NEW;
                                END;
                                $$ LANGUAGE plpgsql;"""

    LOG_AFTER_UPDATE_FUNC = """CREATE OR REPLACE FUNCTION log_after_update() RETURNS TRIGGER AS $$
                                BEGIN
                                    INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                                    VALUES (
                                        TG_TABLE_NAME,
                                        'UPDATE',
                                        row_to_json(OLD)::jsonb,
                                        row_to_json(NEW)::jsonb
                                    );
                                    RETURN NEW;
                                END;
                                $$ LANGUAGE plpgsql;"""

    LOG_AFTER_DELETE_FUNC = """CREATE OR REPLACE FUNCTION log_after_delete() RETURNS TRIGGER AS $$
                                BEGIN
                                    INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
                                    VALUES (
                                        TG_TABLE_NAME,
                                        'DELETE',
                                        row_to_json(OLD)::jsonb,
                                        NULL
                                    );
                                    RETURN OLD;
                                END;
                                $$ LANGUAGE plpgsql;"""

    FUNC_EXISTS_CHECK = """SELECT 
    EXISTS (
        SELECT 1 
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.proname = '{function_name}'  -- Replace with your function name
        AND n.nspname = '{schema_name}'     -- Replace with your schema name
        -- Uncomment next line if you want to check argument types
        -- AND pg_catalog.pg_get_function_identity_arguments(p.oid) = 'arg1_type, arg2_type'
    );"""

    INSERT_TRIGGER = """CREATE TRIGGER after_{table_name}_insert
                         AFTER INSERT ON {table_name}
                         FOR EACH ROW EXECUTE FUNCTION log_after_insert();"""

    UPDATE_TRIGGER = """CREATE TRIGGER after_{table_name}_update
                        AFTER UPDATE ON {table_name}
                        FOR EACH ROW EXECUTE FUNCTION log_after_update();"""

    DELETE_TRIGGER = """CREATE TRIGGER after_{table_name}_delete
                        AFTER DELETE ON {table_name}
                        FOR EACH ROW EXECUTE FUNCTION log_after_delete();"""

    _GET_TRIGGER_INFO = """SELECT
            tgname AS TriggerName,
            tgisinternal AS IsInternal,
            n.nspname AS SchemaName,
            c.relname AS TableName,
            CASE 
                WHEN tgtype & 1 = 1 THEN 'AFTER' 
                WHEN tgtype & 2 = 2 THEN 'BEFORE' 
                ELSE 'INSTEAD OF' 
            END AS TriggerTiming,
            tgfoid::regproc AS TriggerFunction,
            tgrelid::regclass::text AS TriggeredTable,
            pg_catalog.pg_get_triggerdef(t.oid, true) AS TriggerDefinition,
            t.tgparentid AS ParentTriggerId,
            pg_catalog.pg_get_userbyid(t.tgowner) AS Owner
        FROM 
            pg_trigger AS t
        JOIN 
            pg_class AS c ON t.tgrelid = c.oid
        JOIN 
            pg_namespace AS n ON c.relnamespace = n.oid
        WHERE 
            NOT t.tgisinternal
        ORDER BY 
            tgname;"""

    @abstractmethod
    def _connect(self):
        ...


class PostgresHelper(BaseConnectionAttributes):
    """
    PostgresHelper class provides utility functions to interact with a PostgreSQL database.
    It allows managing schema choices, constructing schema-aware SQL queries,
    and handling connections securely.

    Attributes:
        _INSTANCE_DEFAULT: Stores the default instance identifier for connections.
        _DEFAULT_PORT: The default port used to connect to PostgreSQL instances.
        VALID_SCHEMA_CHOICES_QUERY: SQL query to fetch valid schema choices.
        _DEFAULT_SCHEMA_CHOICE: The default schema choice used when no schema is explicitly specified.

    Methods:
        __init__(server, database, **kwargs):
            Initializes the PostgresHelper object, setting up server and database connection details,
            along with any additional configuration passed via kwargs.

        __version__:
            Returns the version of the current PostgresHelper utility as a property.

        initialize_schema_choices(**kwargs):
            Initializes the schema choices by establishing a database connection
            and assigning a schema choice based on the provided arguments or default.

        _connect():
            Establishes a connection to the PostgreSQL database using provided credentials.
            Logs the output to notify upon successful connection.

        _add_schema_to_query(sql_string: str):
            Adds a schema to SQL queries if not already present in the query.
            Ensures schema-awareness for subsequent query executions.

        query(sql_string: str, **kwargs):
            Executes a SQL query, adding schema where necessary. Extends the `query` method
            from the base class to ensure schema information is incorporated.

        valid_schema_choices:
            Property that retrieves valid schema choices from the database
            using the predefined `VALID_SCHEMA_CHOICES_QUERY`.
            Caches the results for subsequent accesses.

        schema_choice:
            Property to get or set the current schema choice.
            Setting a schema validates it against `valid_schema_choices`
            to ensure it is a recognized schema. Raises a ValueError for invalid choices.
    """
    _INSTANCE_DEFAULT = None
    _DEFAULT_PORT = 5432
    VALID_SCHEMA_CHOICES_QUERY = """SELECT schema_name FROM information_schema.schemata;"""
    _DEFAULT_SCHEMA_CHOICE = 'public'

    def __init__(self, server, database, **kwargs):
        self.instance = kwargs.get('instance', self.__class__._INSTANCE_DEFAULT)

        super().__init__(server, database,
                         instance=self.instance, **kwargs)

        self._valid_schema_choices = None
        self._schema_choice = None
        self.initialize_schema_choices(**kwargs)

    @property
    def __version__(self):
        return "0.1"

    def initialize_schema_choices(self, **kwargs):
        """
        Initializes schema choices for the current instance.

        :param kwargs: Optional keyword arguments. It may include 'schema_choice' to specify the schema choice. If not provided, the default schema choice will be used.
        :type kwargs: dict
        :return: None
        :rtype: None
        """
        self.get_connection_and_cursor()
        self.schema_choice = kwargs.get('schema_choice', self.__class__._DEFAULT_SCHEMA_CHOICE)
        self._force_connection_closed()

    def _connect(self):
        """
        Establishes a connection to a PostgreSQL database using the configured server, port, database name, username, and password. Logs messages indicating the status of the connection process.

        :return: A connection object to the PostgreSQL database
        :rtype: psycopg.Connection
        """
        cxn_params = {'host': self.server,
                      'port': self.port,
                      'dbname': self.database,
                      'user': self.username,
                      'password': self._password}
        print("attempting to connect to postgres")
        cxn = psycopg.connect(**cxn_params)
        print("connection successful")
        self._logger.debug("connection successful")
        return cxn

    def _add_schema_to_query(self, sql_string: str):
        """
        :param sql_string: The SQL query string that potentially lacks schema information and needs to be processed.
        :type sql_string: str
        :return: The updated SQL query string with the schema added if it wasn't already specified.
        :rtype: str
        """
        from_statements = [x.strip() for x in sql_string.lower().split('from')][-1].split(',')
        has_schema = [x for x in from_statements if x.find('.') != -1]
        if has_schema:
            self._logger.debug(f"Query already contains schema: {has_schema}")
            return sql_string
        new_from_statements = {x.strip(): '.'.join((self.schema_choice, x.strip())) for x in from_statements}
        sql_string = sql_string.replace(from_statements[0], new_from_statements.get(from_statements[0]))
        self._logger.debug(f"Added schema to query: {sql_string}")
        return sql_string

    def query(self, sql_string: str, **kwargs):
        """
        :param sql_string: The SQL query string that needs to be executed.
        :type sql_string: str
        :param kwargs: Additional keyword arguments that may be required for the query execution.
        :return: The result of executing the modified SQL query.
        :rtype: Any
        """
        super().query(self._add_schema_to_query(sql_string), **kwargs)

    @property
    def valid_schema_choices(self):
        """
        Returns the valid schema choices after querying the database if not already set.

        The method checks if `_valid_schema_choices` is empty. If it is, a database query
        is executed using the `VALID_SCHEMA_CHOICES_QUERY` constant. The results of the
        query are processed, and the first element of each result is extracted to form
        the list of valid schema choices, which is then cached in `_valid_schema_choices`.

        :return: The list of valid schema choices.
        :rtype: list
        """
        if not self._valid_schema_choices:
            self.query(self.__class__.VALID_SCHEMA_CHOICES_QUERY, silent_process=True)
            if self.query_results:
                self._valid_schema_choices = [x[0] for x in self.query_results]
        return self._valid_schema_choices

    @property
    def schema_choice(self):
        """
        :return: The current value of the _schema_choice attribute.
        :rtype: Any
        """
        return self._schema_choice

    @schema_choice.setter
    def schema_choice(self, value):
        """
        :param value: The schema choice to be set. This value must exist within the valid_schema_choices list.
        :type value: str
        :raises ValueError: If the provided value is not within the valid_schema_choices list.
        """
        if value in self.valid_schema_choices:
            self._schema_choice = value
        else:
            raise ValueError(f"Invalid schema choice: {value}. "
                             f"Valid choices are: {self.valid_schema_choices}")


class PostgresHelperTT(PostgresHelper, _PostgresTableTracker, metaclass=ABCPostgresCreateTriggers):
    """
        A helper class that extends `PostgresHelper` and `_PostgresTableTracker`,
        implementing trigger creation functionality via a metaclass.

        This class leverages PostgreSQL for database operations and provides
        functionality to track specified tables, check for the existence of certain
        stored functions, and create them if necessary. It also includes utilities
        for auto-generation and tracking of stored function names based on specific
        class attributes.
    """
    _ATTR_SUFFIX = '_FUNC'
    _ATTR_PREFIX = 'LOG_AFTER_'
    _FUNC_EXISTS_PLACEHOLDER_FN = 'function_name'
    _FUNC_EXISTS_PLACEHOLDER_SCHEMA = 'schema_name'
    _DEFAULT_SCHEMA_CHOICE = 'public'

    def __init__(self, server, database, **kwargs):
        super().__init__(server, database, **kwargs)
        _PostgresTableTracker.__init__(self, **kwargs)

        # the name of the class attr, and the name of the psql function as a tuple
        self._psql_function_attrs_func_name = [(x, self.__class__._format_func_name(x)) for x
                                               in self.__dir__() if self.__class__._is_func_attr(x)]
        self._check_or_create_functions()

    def __new__(cls, *args, **kwargs):
        if cls.TABLES_TO_TRACK == [cls._MAGIC_IGNORE_STRING]:
            raise NoTrackedTablesError(class_name=cls.__name__)
        return super().__new__(cls)

    @property
    def __version__(self):
        return "0.1"

    @classmethod
    def _format_func_name(cls, name: str):
        """
        Formats a given function name by removing the class attribute suffix and converting it to lowercase.

        :param name: The name of the function to be formatted.
        :type name: str
        :return: The formatted function name in lowercase without the class attribute suffix.
        :rtype: str
        """
        return ''.join(name.split(cls._ATTR_SUFFIX)[0].lower())  # + '()'

    @classmethod
    def _is_func_attr(cls, name):
        """
        Checks if a given attribute name is a valid function attribute by verifying
        if it starts and ends with predefined class-specific prefixes and suffixes.

        :param name: The attribute name to check.
        :type name: str
        :return: True if the name matches the function attribute criteria, otherwise False.
        :rtype: bool
        """
        return (name.startswith(cls._ATTR_PREFIX)
                and name.endswith(cls._ATTR_SUFFIX))

    @classmethod
    def _get_func_exists_check_str(cls, func_name, schema_choice):
        """
        :param func_name: The name of the function to be checked for existence.
        :type func_name: str
        :param schema_choice: The name of the schema in which the function existence is to be checked.
        :type schema_choice: str
        :return: A formatted string for checking the existence of the provided function within the specified schema.
        :rtype: str
        """
        return cls.FUNC_EXISTS_CHECK.format(
            **{cls._FUNC_EXISTS_PLACEHOLDER_FN: func_name,
               cls._FUNC_EXISTS_PLACEHOLDER_SCHEMA: schema_choice}
        )

    def _check_or_create_functions(self, **kwargs):
        """
        :param kwargs:
            Dictionary of keyword arguments. Includes:
                - schema_choice: The schema choice to be used for function creation or existence check;
                  defaults to the class-level _DEFAULT_SCHEMA_CHOICE if not provided.
        :return: None
        :rtype: None
        """
        schema_choice = kwargs.get('schema_choice', self.__class__._DEFAULT_SCHEMA_CHOICE)
        for f in self._psql_function_attrs_func_name:
            sql_q = self._get_func_exists_check_str(
                func_name=f[1],
                schema_choice=schema_choice)
            self.query(sql_q, silent_process=True)
            exists = bool(self.query_results)
            if not exists:
                self._logger.info(f"Creating function {f[1]}")
                self.query(getattr(self.__class__, f[0]), is_commit=True)
            self._logger.debug(f"Function {f[1]} exists: {exists}")


if __name__ == '__main__':
    pg = PostgresHelperTT('192.168.1.7',  # port=5432,
                          database='postgres')#,
                          #username='postgres')
