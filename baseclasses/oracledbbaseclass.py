import oracledb

from configparser import ConfigParser
from logger import ServiceLogger

oracledb.init_oracle_client(config_dir='/etc/tnsnames.ora')

_logger = ServiceLogger("oracledb", __file__, 'ERROR').logger
class OracleDbBaseClass:

    def __init__(self, path):
        self.connection = self.__make_connection(path)
        self.path = path

    # private method
    def __make_connection(self, path):
        """
        Create a database connection to the PanDA database
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            dbuser = cfg.get('pandadb', 'login')
            dbpasswd = cfg.get('pandadb', 'password')
            description = cfg.get('pandadb', 'description')
        except:
            pass
        try:
            connection = oracledb.connect(user=dbuser, password=dbpasswd, dsn=description)
            return connection
        except Exception as ex:
            _logger.error(ex)
        return None