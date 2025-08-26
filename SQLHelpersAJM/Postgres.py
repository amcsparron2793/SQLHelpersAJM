from abc import abstractmethod
from getpass import getpass

import psycopg
from SQLHelpersAJM import BaseConnectionAttributes, BaseCreateTriggers
from backend import ABCPostgresCreateTriggers


# noinspection SqlNoDataSourceInspection
class _PostgresTableTracker(BaseCreateTriggers):
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

    @abstractmethod
    def _connect(self):
        ...


class PostgresHelper(BaseConnectionAttributes):
    _INSTANCE_DEFAULT = None
    _DEFAULT_PORT = 5432
    VALID_SCHEMA_CHOICES_QUERY = """SELECT schema_name FROM information_schema.schemata;"""
    _DEFAULT_SCHEMA_CHOICE = 'public'
    _GET_PASS_PROMPT = "Enter password for database user '{}' (no output will show on screen): "

    def __init__(self, server, database, **kwargs):
        self.instance = kwargs.get('instance', self.__class__._INSTANCE_DEFAULT)

        super().__init__(server, database,
                         instance=self.instance, **kwargs)

        self.port = kwargs.get('port', self.__class__._DEFAULT_PORT)
        self.username = kwargs.get('username', '')
        self._password = kwargs.get('password',
                                    None)
        if not self._password:
            self._password = getpass(self.__class__._GET_PASS_PROMPT.format(self.username))

        self._valid_schema_choices = None
        self._schema_choice = None
        self.initialize_schema_choices(**kwargs)

    def initialize_schema_choices(self, **kwargs):
        self.get_connection_and_cursor()
        self.schema_choice = kwargs.get('schema_choice', self.__class__._DEFAULT_SCHEMA_CHOICE)
        self._force_connection_closed()

    def _connect(self):
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
        super().query(self._add_schema_to_query(sql_string), **kwargs)

    @property
    def valid_schema_choices(self):
        if not self._valid_schema_choices:
            self.query(self.__class__.VALID_SCHEMA_CHOICES_QUERY, silent_process=True)
            if self.query_results:
                self._valid_schema_choices = [x[0] for x in self.query_results]
        return self._valid_schema_choices

    @property
    def schema_choice(self):
        return self._schema_choice

    @schema_choice.setter
    def schema_choice(self, value):
        if value in self.valid_schema_choices:
            self._schema_choice = value
        else:
            raise ValueError(f"Invalid schema choice: {value}. "
                             f"Valid choices are: {self.valid_schema_choices}")


class PostgresHelperTT(PostgresHelper, _PostgresTableTracker, metaclass=ABCPostgresCreateTriggers):
    TABLES_TO_TRACK = ['test_table']
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

    @classmethod
    def _format_func_name(cls, name: str):
        return ''.join(name.split(cls._ATTR_SUFFIX)[0].lower())  # + '()'

    @classmethod
    def _is_func_attr(cls, name):
        return (name.startswith(cls._ATTR_PREFIX)
                and name.endswith(cls._ATTR_SUFFIX))

    @classmethod
    def _get_func_exists_check_str(cls, func_name, schema_choice):
        return cls.FUNC_EXISTS_CHECK.format(
            **{cls._FUNC_EXISTS_PLACEHOLDER_FN: func_name,
               cls._FUNC_EXISTS_PLACEHOLDER_SCHEMA: schema_choice}
        )

    def _check_or_create_functions(self, **kwargs):
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
                          database='postgres',
                          username='postgres')
