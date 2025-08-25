from abc import abstractmethod

import psycopg
from SQLHelpersAJM import BaseConnectionAttributes, BaseCreateTriggers
from backend import ABCPostgresCreateTriggers


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

    # TODO: should AUDIT_LOG_CREATED_CHECK be a straight select query?
    AUDIT_LOG_CREATED_CHECK = """DO $$
                                    BEGIN
                                        IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'audit_log') THEN
                                            RAISE NOTICE 'Audit log table not found.';
                                        END IF;
                                    END;
                                    $$;"""

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
        WHERE p.proname = 'function_name'  -- Replace with your function name
        AND n.nspname = 'schema_name'     -- Replace with your schema name
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

    def __init__(self, server, database, **kwargs):
        self.instance = kwargs.get('instance', self.__class__._INSTANCE_DEFAULT)

        super().__init__(server, database,
                         instance=self.instance, **kwargs)

        self.port = kwargs.get('port', self.__class__._DEFAULT_PORT)
        self.username = kwargs.get('username', '')
        self._password = kwargs.get('password', '')

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


class PostgresHelperTT(PostgresHelper, _PostgresTableTracker, metaclass=ABCPostgresCreateTriggers):
    TABLES_TO_TRACK = ['test_table']

    def __init__(self, server, database, **kwargs):
        super().__init__(server, database, **kwargs)
        _PostgresTableTracker.__init__(self, **kwargs)
        # the name of the class attr, and the name of the psql function as a tuple
        self._psql_function_attrs_func_name = [(x, (''.join(x[1:]).split('_FUNC')[0].lower()) + '()')
                                               for x in self.__dir__() if x.startswith('LOG_AFTER_')
                                               and x.endswith('_FUNC')]
        self._check_or_create_functions()

    def _check_or_create_functions(self):
        for f in self._psql_function_attrs_func_name:
            self.query(self.FUNC_EXISTS_CHECK.replace(
                'function_name', f[1]).replace('schema_name', 'public'))
            exists = bool(self.query_results)
            if not exists:
                self._logger.info(f"Creating function {f[1]}")
                self.query(getattr(self.__class__, f[0]), is_commit=True)
            self._logger.debug(f"Function {f[1]} exists: {exists}")


if __name__ == '__main__':
    pg = PostgresHelperTT('192.168.1.7',  # port=5432,
                          database='postgres',
                          username='postgres',
                          password=input('Enter Postgres db password for: '))
    pg.get_connection_and_cursor()
    pg.query("select * from pi.test_table;")
    print(pg.query_results)
