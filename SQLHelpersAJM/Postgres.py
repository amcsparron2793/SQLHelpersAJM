import psycopg
from SQLHelpersAJM import BaseConnectionAttributes


class PostgresHelper(BaseConnectionAttributes):
    _INSTANCE_DEFAULT = None
    _DEFAULT_PORT = 5432

    def __init__(self, server, database,  **kwargs):
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
            'password':self._password}
        print("attempting to connect to postgres")
        cxn = psycopg.connect(** cxn_params)
        print("connection successful")
        self._logger.debug("connection successful")
        return cxn

if __name__ == '__main__':
    pg = PostgresHelper('192.168.1.7',# port=5432,
                        database='postgres',
                        username='postgres', password=input('password: '))
    pg.get_connection_and_cursor()
    pg.query("select * from pi.test_table;")
    print(pg.query_results)
