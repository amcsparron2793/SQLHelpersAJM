import psycopg2
from SQLHelpersAJM import _BaseSQLHelper

class PostgresHelper(_BaseSQLHelper):
    def __init__(self, host, port, database, user, password, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def _connect(self):
        cxn_params = {'host': self.host,
            'port': self.port,
            'dbname': self.database,
            'user': self.user,
            'password':self.password}
        print("attempting to connect to postgres")
        self._logger.debug(f"attempting to connect to postgres with "
                           f"the following parameters: {cxn_params}")
        cxn = psycopg2.connect(** cxn_params)
        print("connection successful")
        self._logger.debug("connection successful")
        return cxn

if __name__ == '__main__':
    pg = PostgresHelper('192.168.1.17', 5432,
                        'postgres',
                        'pi', input('password: '))
    pg.get_connection_and_cursor()
    #pg.query("select * from test_table;")
    print(pg.query_results)