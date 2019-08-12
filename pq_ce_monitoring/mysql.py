import requests
import pandas as pd

from configparser import ConfigParser
from datetime import datetime
from es import Es

from error_accounting import Errors
from logger import ServiceLogger

_logger = ServiceLogger("mysql").logger

class Mysql:
    def __init__(self, path):
        self.connection = self.__make_connection(path=path)
        self.path = path

    # private method
    def __make_connection(self, path):
        """
        Create a connection to MySQL
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            user = cfg.get('mysql', 'login')
            password = cfg.get('mysql', 'password')
            dbname = cfg.get('mysql', 'dbname')
            host = cfg.get('mysql', 'host')

        except Exception as ex:
            _logger.error(ex.message)
            print ex.message
        try:
            from sqlalchemy import create_engine
            con = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(user, password,host, dbname))
            return con
        except Exception as ex:
            _logger.error(ex.message)
            print ex.message
        return None

    def write_data(self, tdelta):

        date_key = datetime.now()

        es = Es(self.path)

        s = es.get_ratio(tdelta)

        harvester_queues = {}
        harvester_queues_errors = {}
        harvester_queues_errors_list_df = []

        harvester_queues_list_df = []

        for hit in s.aggregations.computingsite:
            harvester_queues[hit.key] = {'totalworkers': hit.doc_count,
                                        'goodworkers': hit.workerstats.buckets['good']['doc_count'],
                                        'badworkers': hit.workerstats.buckets['bad']['doc_count'],
                                        'ratio': (1 - (float(hit.workerstats.buckets['bad']['doc_count']) / float(
                                            hit.doc_count))) * 100}
            if int(hit.workerstats.buckets['bad']['doc_count']) > 0:
                errors_object = Errors('patterns.txt')
                harvester_queues_errors = errors_object.accounting(hit, harvester_queues_errors)


        harvester_computingelements_list_df = []

        for hit in s.aggregations.computingelement:
            harvester_computingelements_list_df.append(
                [date_key.strftime("%Y-%m-%dT%H:%M")[:-1]+'0:00', hit.key, hit.doc_count, hit.workerstats.buckets['good']['doc_count'],
                 hit.workerstats.buckets['bad']['doc_count'],
                 (1 - (float(hit.workerstats.buckets['bad']['doc_count']) / float(hit.doc_count))) * 100
                 ])

        q_keys = harvester_queues.keys()
        q_pq_keys = harvester_queues_errors.keys()

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        for queuename, queue in queues.iteritems():
            if queuename in q_keys:
                harvester_queues_list_df.append([date_key.strftime("%Y-%m-%dT%H:%M")[:-1]+'0:00', queuename,
                                                 queue['cloud'],
                                                 queue['atlas_site'],
                                                 harvester_queues[queuename]['totalworkers'],
                                                 harvester_queues[queuename]['goodworkers'],
                                                 harvester_queues[queuename]['badworkers'],
                                                 harvester_queues[queuename]['ratio'],
                                                 queue['status'],
                                                 queue['harvester']
                                                 ])
                if queuename in q_pq_keys:
                    for error in harvester_queues_errors[queuename]:
                        harvester_queues_errors_list_df.append([date_key.strftime("%Y-%m-%dT%H:%M")[:-1]+'0:00',
                                                     queue['cloud'],
                                                     queue['atlas_site'],
                                                     queuename,
                                                     error,
                                                     harvester_queues_errors[queuename][error]['error_count'],
                                                     queue['harvester']
                                                     ])
            else:
                harvester_queues_list_df.append(
                    [date_key.strftime("%Y-%m-%dT%H:%M")[:-1]+'0:00', queuename,
                     queue['cloud'],
                     queue['atlas_site'],
                     0, 0, 0, 0, queue['status'], queue['harvester']])

        try:
            frame_computingsite = pd.DataFrame(harvester_queues_list_df, columns=["timestamp", "computingsite","cloud","site","totalworkers","goodworkers","badworkers","ratio","status","harvesterid"])
            frame_computingsite.to_sql(con=self.connection, name='Computingsite', if_exists='append', index=False, chunksize=1)
        except Exception as ex:
            _logger.error(ex)
        try:
            frame_computingsite_errors = pd.DataFrame(harvester_queues_errors_list_df, columns=["timestamp", "cloud","site","computingsite","errordesc","errorcount","harvesterid"])
            frame_computingsite_errors.to_sql(con=self.connection, name='Pqerrors', if_exists='append', index=False, chunksize=1)
        except Exception as ex:
            _logger.error(ex)