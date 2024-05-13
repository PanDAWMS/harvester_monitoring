import json
from logger import ServiceLogger
from baseclasses.oracledbbaseclass import OracleDbBaseClass

_logger = ServiceLogger("pandadb", __file__).logger


class PandaDB(OracleDbBaseClass):

    def __init__(self, path):
        super().__init__(path)

    def get_db_metrics(self):
        """
        Get metrics from PandaDB
        """
        try:
            connection = self.connection
            metrcis = {}

            query = """
            SELECT t.harvester_id, t.harvester_host, t.CREATION_TIME, t.METRICS FROM ( 
            SELECT harvester_id, harvester_host, MAX(creation_time) AS CREATION_TIME
            FROM atlas_panda.harvester_metrics
            GROUP BY harvester_id, harvester_host) x 
            JOIN atlas_panda.harvester_metrics t ON x.harvester_id = t.harvester_id
            AND x.harvester_host = t.harvester_host AND x.CREATION_TIME = t.CREATION_TIME
            """

            results = self.__read_query(query, connection)

            for row in results:
                if row['harvester_id'] not in metrcis:
                    metrcis[row['harvester_id']] = {}
                if row['harvester_host'] not in metrcis[row['harvester_id']]:
                    metrcis[row['harvester_id']][row['harvester_host']] = {}
                try:
                    metrcis[row['harvester_id']][row['harvester_host']].setdefault(row['creation_time'],
                                                                                     []).append(json.loads(row['metrics']))
                except Exception as ex:
                    print(ex)
                    _logger.error(row['harvester_id'] + ' ' + row['harvester_host'] + ' ' + row['metrics'])
            _logger.debug("Metrics: {0}".format(str(metrcis)))
            return metrcis
        except Exception as ex:
            _logger.error(ex)
            print(ex)
    def get_last_worker(self, type='harvesterhost'):
        """
        Get last submitted worker from PandaDB
        """
        try:
            connection = self.connection
            hosts = {}

            query = """
                SELECT
                  CASE
                    WHEN INSTR({0}, ',') > 0 THEN
                      SUBSTR({0}, 1, INSTR({0}, ',') - 1)
                    ELSE
                      {0}
                  END AS {0},
                  MAX(submittime) AS last_submittime
                FROM
                  ATLAS_PANDA.HARVESTER_WORKERS
                WHERE
                  SUBMITTIME >= CAST(sys_extract_utc(SYSTIMESTAMP) - INTERVAL '1' DAY AS DATE)
                GROUP BY
                  {0}
            """.format(type)

            results = self.__read_query(query, connection)

            for row in results:
                if row[type] not in hosts:
                    hosts[row[type]] = {}
                hosts[row[type]] = row['last_submittime']

            return hosts

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
