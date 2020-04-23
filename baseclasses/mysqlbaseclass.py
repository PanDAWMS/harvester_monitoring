from configparser import ConfigParser
from sqlalchemy import create_engine

from logger import ServiceLogger

_logger = ServiceLogger("mysql", __file__, 'ERROR').logger

class MySQLBaseClass:

    def __init__(self, path):
        self.connection = self.__make_connection(path=path)
        self.path = path

    # private method
    def __make_connection(self, path):
        """
        Create a connection to InfluxDB
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            user = cfg.get('mysql', 'login')
            password = cfg.get('mysql', 'password')
            dbname = cfg.get('mysql', 'dbname')
            host = cfg.get('mysql', 'host')

        except Exception as ex:
            _logger.error(ex)
            print(ex)
        try:
            string_connection = """mysql+pymysql://{0}:{1}@{2}/{3}""".format(user, password, host, dbname)
            connection = create_engine(string_connection)
            return connection
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        return None