import psycopg2
from SQLHelpersAJM import _BaseConnectionAttributes

class PostgresHelper(_BaseConnectionAttributes):
    def __init__(self, server, database, **kwargs):
        super().__init__(server, database, **kwargs)
        self.port = kwargs.get('port', 5432)
        self.username = kwargs.get('username', 'postgres')
        self._password = kwargs.get('password', '')

    def _connect(self):
        cxn_params = {'host': self.server,
            'port': self.port,
            'dbname': self.database,
            'user': self.username,
            'password':self._password}
        print("attempting to connect to postgres")
        self._logger.debug(f"attempting to connect to postgres with "
                           f"the following parameters: {cxn_params}")
        cxn = psycopg2.connect(** cxn_params)
        print("connection successful")
        self._logger.debug("connection successful")
        return cxn

if __name__ == '__main__':
    pg = PostgresHelper('192.168.1.17', port=5432,
                        database='postgres',
                        username='pi', password=input('password: '))
    pg.get_connection_and_cursor()
    #pg.query("select * from test_table;")
    print(pg.query_results)