import psycopg2
from SQLHelpersAJM import BaseConnectionAttributes


class PostgresHelper(BaseConnectionAttributes):
    def __init__(self, server, database, **kwargs):
        self._logger = self._setup_logger(basic_config_level='DEBUG')
        super().__init__(server, database, logger=self._logger, **kwargs)
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