from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from datetime import datetime, timedelta
from logger import ServiceLogger

_logger = ServiceLogger("configuration").logger

class Es:

    def __init__(self, path):
        self.connection = self.__make_connection(path= path)

    # private method
    def __make_connection(self, path, use_ssl=True, verify_certs=False, timeout=30, max_retries=10,
                             retry_on_timeout=True):
        """
        Create a connection to ElasticSearch cluster
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            eslogin = cfg.get('server', 'login')
            espasswd = cfg.get('server', 'password')
            host = cfg.get('server', 'host')
            port = cfg.get('server', 'port')
        except Exception as ex:
            _logger.error(ex.message)
            print ex.message
        try:
            connection = Elasticsearch(
                [{'host': host, 'port': int(port)}],
                http_auth=(eslogin, espasswd),
                use_ssl=use_ssl,
                verify_certs=verify_certs,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout,
            )
            return connection
        except Exception as ex:
            _logger.error(ex.message)
            print ex.message
        return None

    def get_last_submittedtime(self):
        """
        Get last submitted time for harvester instances and harvester hosts
        """
        connection = self.connection

        s = Search(using=connection, index='atlas_harvesterworkers-*')[:0]

        s.aggs.bucket('harvesterid', 'terms', field='harvesterid.keyword', size=1000) \
            .metric('max_submittime', 'max', field='submittime') \
            .metric('min_submittime', 'min', field='submittime') \
            .bucket('harvesterhost', 'terms', field='harvesterhost.keyword', order={'max_hostsubmittime': 'desc'},
                    size=100) \
            .metric('max_hostsubmittime', 'max', field='submittime')

        s = s.execute()

        harvesterhostDict = {}
        harvesteridDict = {}

        for hit in s.aggregations.harvesterid:
            harvesteridDict[hit.key] = {
                'max': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z'),
                'min': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z')
            }
            if len(hit.harvesterhost) == 0:
                harvesteridDict[hit.key]['harvesterhost'] = {}
                harvesteridDict[hit.key]['harvesterhost']['none'] = {
                    'harvesterhostmaxtime': datetime.strptime(hit.max_submittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z'),
                    'harvesterhostmintime': datetime.strptime(hit.max_submittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z')}
            for harvesterhost in hit.harvesterhost:
                if 'harvesterhost' not in harvesteridDict[hit.key]:
                    harvesteridDict[hit.key]['harvesterhost'] = {}
                harvesteridDict[hit.key]['harvesterhost'][harvesterhost.key] = {
                    'harvesterhostmaxtime': datetime.strptime(harvesterhost.max_hostsubmittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z')}
        return harvesterhostDict, harvesteridDict

    def get_exceptions(self):
        """
        TODO: need to test
        """
        s = Search(using=self.connection, index='atlas_harvesterlogs-*').filter('range', **{
            '@timestamp': {'gte': 'now-30m', 'lte': 'now'}}).filter('terms', tags=['error'])
        s = s.scan()
        return None

    def get_schedd_metrics(self):
        """
        Get last schedd metrics
        """
        connection = self.connection

        s = Search(using=connection, index='atlas_scheddmetrics-*')[:0]

        s.aggs.bucket('submissionhost', 'terms', field='submissionhost.keyword', size=1000) \
        .metric('last_entry', 'top_hits', size=1, _source=['creation_time','condor_collector','condor_negotiator',
                                                     'condor_schedd','disk_usage_cephfs','disk_usage_data',
                                                     'disk_usage_data1','disk_usage_data1','disk_usage_data2',
                                                     'n_workers_created_total'
                                                     ], sort=[{'creation_time': 'desc'}])\
        .metric('creation_time', 'max', field='creation_time')

        s = s.execute()

        submissionhostDict = {}

        for hit in s.aggregations.submissionhost:
            submissionhostDict[hit.key] = {}
            for key in hit.last_entry.hits.hits[0]['_source']:
                submissionhostDict[hit.key][key] = hit.last_entry.hits.hits[0]['_source'][key]

        s = Search(using=connection, index='atlas_harvesterworkers-*')[:0]

        s.aggs.bucket('submissionhost', 'terms', field='submissionhost.keyword', size=1000) \
        .metric('max_submittime', 'max', field='submittime')
        s = s.execute()

        for hit in s.aggregations.submissionhost:
            key = str(hit.key).split(',')[0]
            if key in submissionhostDict:
                submissionhostDict[key]['last_submittime'] = datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z')
        return submissionhostDict