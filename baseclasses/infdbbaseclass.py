from configparser import ConfigParser
from influxdb import InfluxDBClient

from logger import ServiceLogger

_logger = ServiceLogger("influxdb", __file__, 'ERROR').logger

class InfluxDbBaseClass:

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
            user = cfg.get('influxdb', 'login')
            password = cfg.get('influxdb', 'password')
            dbname = cfg.get('influxdb', 'dbname')
            host = cfg.get('influxdb', 'host')
            port = cfg.get('influxdb', 'port')

        except Exception as ex:
            _logger.error(ex)
            print(ex)
        try:
            connection = InfluxDBClient(host, int(port), user, password, dbname, ssl=True,
                                        verify_ssl=False)
            return connection
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        return None