import cx_Oracle, json

from configparser import ConfigParser
from logger import ServiceLogger

_logger = ServiceLogger("pandadb", __file__).logger


class PandaDB:

    def __init__(self, path):
        self.connection = self.__make_connection(path)

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
            connection = cx_Oracle.connect(dbuser, dbpasswd, description)
            return connection
        except Exception as ex:
            _logger.error(ex)
        return None

    def get_db_metrics(self):
        """
        Get metrics from PandaDB
        """
        try:
            connection = self.connection
            metrcis = {}
            query = """SELECT t.harvester_id, t.harvester_host, t.CREATION_TIME, t.METRICS FROM ( 
            SELECT harvester_id, harvester_host, MAX(creation_time) AS CREATION_TIME
            FROM atlas_panda.harvester_metrics
            GROUP BY harvester_id, harvester_host) x 
            JOIN atlas_panda.harvester_metrics t ON x.harvester_id =t.harvester_id
            AND x.harvester_host = t.harvester_host AND x.CREATION_TIME = t.CREATION_TIME"""

            results = self.__read_query(query, connection)
            for row in results:
                if row['harvester_id'] not in metrcis:
                    metrcis[row['harvester_id']] = {}
                if row['harvester_host'] not in metrcis[row['harvester_id']]:
                    metrcis[row['harvester_id']][row['harvester_host']] = {}
                metrcis[row['harvester_id']][row['harvester_host']].setdefault(row['creation_time'],
                                                                                     []).append(
                    json.loads(row['metrics']))
            _logger.debug("Metrics: {0}".format(str(metrcis)))
            return metrcis
        except Exception as ex:
            _logger.error(ex)
            print(ex)

    # private method
    def __read_query(self, query, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return self.__rows_to_dict_list(cursor)
        finally:
            if cursor is not None:
                cursor.close()

    # private method
    def __rows_to_dict_list(self, cursor):
        columns = [str(i[0]).lower() for i in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]
