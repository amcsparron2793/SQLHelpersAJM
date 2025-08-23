from abc import abstractmethod

import psycopg
from SQLHelpersAJM import BaseConnectionAttributes, BaseCreateTriggers

# TODO: finish this, it needs to be handled differently due to functions
"""Functions for Triggers:
PostgreSQL requires functions to define the logic that is executed by the triggers. These functions handle the operations (INSERT, UPDATE, DELETE) and insert rows into the audit_log table with applicable JSONB data.
Triggers:
The CREATE TRIGGER statements attach the functions to the target table using FOR EACH ROW."""


# -- Creating the `audit_log` table
# CREATE TABLE audit_log (
#     id SERIAL PRIMARY KEY,
#     table_name TEXT NOT NULL,
#     operation TEXT NOT NULL,
#     old_row_data JSONB,
#     new_row_data JSONB,
#     change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
#
# -- Query to check if the `audit_log` table exists
# DO $$
# BEGIN
#     IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'audit_log') THEN
#         RAISE NOTICE 'Audit log table not found.';
#     END IF;
# END;
# $$;
#
# -- Query to check if a trigger exists for a specific table
# DO $$
# DECLARE
#     trigger_found BOOLEAN;
# BEGIN
#     SELECT EXISTS (
#         SELECT 1
#         FROM pg_trigger
#         WHERE tgname = 'after_' || '{table}' || '_insert'
#     ) INTO trigger_found;
#
#     IF trigger_found THEN
#         RAISE NOTICE 'Trigger exists on table {table}.';
#     ELSE
#         RAISE NOTICE 'Trigger does not exist on table {table}.';
#     END IF;
# END;
# $$;
#
# -- Query to get column names of a specific table
# SELECT column_name AS columnName
# FROM information_schema.columns
# WHERE table_name = '{table}';
#
# -- Function for handling inserts
# CREATE OR REPLACE FUNCTION log_after_insert() RETURNS TRIGGER AS $$
# BEGIN
#     INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
#     VALUES (
#         TG_TABLE_NAME,
#         'INSERT',
#         NULL,
#         row_to_json(NEW)::jsonb
#     );
#     RETURN NEW;
# END;
# $$ LANGUAGE plpgsql;
#
# -- Function for handling updates
# CREATE OR REPLACE FUNCTION log_after_update() RETURNS TRIGGER AS $$
# BEGIN
#     INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
#     VALUES (
#         TG_TABLE_NAME,
#         'UPDATE',
#         row_to_json(OLD)::jsonb,
#         row_to_json(NEW)::jsonb
#     );
#     RETURN NEW;
# END;
# $$ LANGUAGE plpgsql;
#
# -- Function for handling deletes
# CREATE OR REPLACE FUNCTION log_after_delete() RETURNS TRIGGER AS $$
# BEGIN
#     INSERT INTO audit_log (table_name, operation, old_row_data, new_row_data)
#     VALUES (
#         TG_TABLE_NAME,
#         'DELETE',
#         row_to_json(OLD)::jsonb,
#         NULL
#     );
#     RETURN OLD;
# END;
# $$ LANGUAGE plpgsql;
#
# -- Creating the INSERT trigger
# CREATE TRIGGER after_{table_name}_insert
# AFTER INSERT ON {table_name}
# FOR EACH ROW EXECUTE FUNCTION log_after_insert();
#
# -- Creating the UPDATE trigger
# CREATE TRIGGER after_{table_name}_update
# AFTER UPDATE ON {table_name}
# FOR EACH ROW EXECUTE FUNCTION log_after_update();
#
# -- Creating the DELETE trigger
# CREATE TRIGGER after_{table_name}_delete
# AFTER DELETE ON {table_name}
# FOR EACH ROW EXECUTE FUNCTION log_after_delete();
class _PostgresTableTracker(BaseCreateTriggers):
    TABLES_TO_TRACK = []
    AUDIT_LOG_CREATE_TABLE = """CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    old_row_data JSONB,
    new_row_data JSONB,
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
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

    INSERT_TRIGGER = None
    UPDATE_TRIGGER = None
    DELETE_TRIGGER = None

    @abstractmethod
    def _connect(self):
        ...


class PostgresHelper(BaseConnectionAttributes):
    _INSTANCE_DEFAULT = None
    _DEFAULT_PORT = 5432

    def __init__(self, server, database, **kwargs):
        self._logger = self._setup_logger(basic_config_level='DEBUG')
        self.instance = kwargs.get('instance', self.__class__._INSTANCE_DEFAULT)

        super().__init__(server, database,
                         instance=self.instance,
                         logger=self._logger, **kwargs)

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


if __name__ == '__main__':
    pg = PostgresHelper('192.168.1.7',  # port=5432,
                        database='postgres',
                        username='postgres', password=input('password: '))
    pg.get_connection_and_cursor()
    pg.query("select * from pi.test_table;")
    print(pg.query_results)
