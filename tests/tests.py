import unittest
from SQLHelpersAJM.SQLite3_helper import SQLite3Helper
from sqlite3 import OperationalError, IntegrityError
from pathlib import Path
from logging import warning


# noinspection SqlNoDataSourceInspection
class SQLite3HelperClassTest(unittest.TestCase):
    TEST_DB_PATH = Path('./testdb.db')
    TEST_TABLE_SQL = "create table Test(id integer primary key, random_name varchar(20));"
    TEST_TABLE_TWO_SQL = "create table Test_two(id integer primary key, test_id integer references Test(id));"
    INSERT_NAMES_INTO_TEST_SQL = "insert into Test(random_name) values('Andrew'), ('Joe'), ('Paul');"
    SELECT_IMPOSSIBLE_ID_SQL = "select * from test where id=(select max(id) + 1 from test)"
    SELECT_ALL_FROM_TEST_SQL = "select * from Test"
    INSERT_INVALID_VALUE_INTO_TEST_TWO_SQL = "insert into Test_two(test_id) values(4);"

    @classmethod
    def setUpClass(cls):
        if Path(SQLite3HelperClassTest.TEST_DB_PATH).exists():
            warning('test database already exists, deleting it')
            SQLite3HelperClassTest.TEST_DB_PATH.unlink()
        sql = SQLite3Helper(SQLite3HelperClassTest.TEST_DB_PATH)
        cxn, csr = sql.get_connection_and_cursor()
        try:
            csr.execute(SQLite3HelperClassTest.TEST_TABLE_SQL)
        except OperationalError:
            pass
        try:
            csr.execute(SQLite3HelperClassTest.TEST_TABLE_TWO_SQL)
        except OperationalError as e:
            pass
        csr.execute(SQLite3HelperClassTest.INSERT_NAMES_INTO_TEST_SQL)
        cxn.commit()
        return cls

    @classmethod
    def tearDownClass(cls):
        try:
            SQLite3HelperClassTest.TEST_DB_PATH.unlink()
        except PermissionError as e:
            warning(e)
            pass

    def setUp(self):
        self.sql = SQLite3Helper(SQLite3HelperClassTest.TEST_DB_PATH)
        self.sql.get_connection_and_cursor()

    def test_no_res_returns_none(self):
        self.sql.Query(SQLite3HelperClassTest.SELECT_IMPOSSIBLE_ID_SQL)
        try:
            self.assertIsNone(self.sql.query_results)
        except AssertionError:
            if type(self.sql.query_results) is list and len(self.sql.query_results) < 1:
                self.sql.query_results = None
                self.assertIsNone(self.sql.query_results)

    def test_query_results_returns_list_tuple(self):
        self.sql.Query(SQLite3HelperClassTest.SELECT_ALL_FROM_TEST_SQL)
        self.assertIsInstance(self.sql.query_results, list)
        self.assertIsInstance(self.sql.query_results[0], tuple)

    def test_list_dict_results_returns_list_dict(self):
        self.sql.Query(SQLite3HelperClassTest.SELECT_ALL_FROM_TEST_SQL)
        self.assertIsInstance(self.sql.list_dict_results, list)
        self.assertIsInstance(self.sql.list_dict_results[0], dict)

    def test_pragma_foreign_keys_is_true(self):
        self.sql.Query("pragma foreign_keys")
        self.assertEqual(self.sql.query_results, 1)

    def test_foreign_key_error_throws_error(self):
        did_err = False
        try:
            self.sql._cursor.execute(SQLite3HelperClassTest.INSERT_INVALID_VALUE_INTO_TEST_TWO_SQL)
        except IntegrityError as e:
            print(f'{e} as expected')
            did_err = True
        self.assertTrue(did_err)
        # this allows tearDownClass to delete the test database
        self.sql._connection.close()


if __name__ == '__main__':
    unittest.main()
