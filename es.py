from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from datetime import datetime, timedelta
from logger import ServiceLogger

_logger = ServiceLogger("es").logger

class Es:

    def __init__(self, path):
        self.connection = self.__make_connection(path=path)

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

    def get_ratio(self, tdelta=60):

        connection = self.connection

        date_UTC = datetime.utcnow()
        date_str = date_UTC - timedelta(minutes=tdelta)

        s = Search(using=connection, index='atlas_harvesterworkers-*')
        s = s.filter('range', **{'endtime': {'gte': date_str.strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00',
                                          'lt': datetime.utcnow().strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00'}})

        s.aggs.bucket('computingsite', 'terms', field='computingsite.keyword', size=50000)
        s.aggs['computingsite'].bucket('workerstats', 'filters',
                                       filters={
                                           'good': Q('terms', status=['finished']),
                                           'bad': Q('terms', status=['cancelled', 'failed', 'missed'])
                                       }
                                       )
        s.aggs['computingsite']['workerstats'].bucket('errors', 'terms', field='diagmessage.keyword', size=50000)

        s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=50000)
        s.aggs['computingelement'].bucket('workerstats', 'filters',
                                          filters={
                                              'good': Q('terms', status=['finished']),
                                              'bad': Q('terms', status=['cancelled', 'failed', 'missed'])
                                          }
                                          )
        s.aggs['computingelement']['workerstats'].bucket('errors', 'terms', field='diagmessage.keyword', size=50000)

        s = s.execute()
        return s

    def get_info_workers(self, tdelta=60):

        connection = self.connection

        date_UTC = datetime.utcnow()
        date_str = date_UTC - timedelta(minutes=tdelta)
        genes_filter = Q('bool', must=[Q('terms', status=['failed','finished','canceled','missed'])])
        s = Search(using=connection, index='atlas_harvesterworkers-*')
        s = s.filter('range', **{'endtime': {'gte': date_str.strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00',
                                          'lt': datetime.utcnow().strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00'}})

        response = s.scan()

        harvester_q = {}
        harvester_ce = {}
        harvester_schedd_ce = {}

        for hit in response:
            submissionhost = hit.submissionhost.split(',')[0]

            if hit.computingsite not in harvester_q:
                harvester_q[hit.computingsite] = {'totalworkers':0,'badworkers':0,'goodworkers':0}

            if hit.computingsite not in harvester_ce:
                harvester_ce[hit.computingsite] = {}
                if hit.computingelement not in harvester_ce[hit.computingsite]:
                    harvester_ce[hit.computingsite][hit.computingelement] = {'totalworkers':0,'badworkers':0,'goodworkers':0}
            else:
                if hit.computingelement not in harvester_ce[hit.computingsite]:
                    harvester_ce[hit.computingsite][hit.computingelement] = {'totalworkers':0,'badworkers':0,'goodworkers':0}

            if submissionhost not in harvester_schedd_ce:
                harvester_schedd_ce[submissionhost] = {}
                if hit.computingelement not in harvester_schedd_ce[submissionhost]:
                    harvester_schedd_ce[submissionhost][hit.computingelement] = {'totalworkers':0,'badworkers':0,'goodworkers':0}
            else:
                if hit.computingelement not in harvester_schedd_ce[submissionhost]:
                    harvester_schedd_ce[submissionhost][hit.computingelement] = {'totalworkers': 0, 'badworkers': 0,
                                                                                 'goodworkers': 0}


            if hit.status in ('failed','missed','cancelled'):

                harvester_q[hit.computingsite]['badworkers'] += 1
                harvester_q[hit.computingsite]['totalworkers'] += 1

                harvester_ce[hit.computingsite][hit.computingelement]['totalworkers'] += 1
                harvester_ce[hit.computingsite][hit.computingelement]['badworkers'] += 1

                harvester_schedd_ce[submissionhost][hit.computingelement]['totalworkers'] += 1
                harvester_schedd_ce[submissionhost][hit.computingelement]['badworkers'] += 1

                if 'errors' not in harvester_q[hit.computingsite]:
                    harvester_q[hit.computingsite]['errors'] = {}

                if 'errors' not in harvester_ce[hit.computingsite][hit.computingelement]:
                    harvester_ce[hit.computingsite][hit.computingelement]['errors'] = {}

                if 'errors' not in harvester_schedd_ce[submissionhost][hit.computingelement]:
                    harvester_schedd_ce[submissionhost][hit.computingelement]['errors'] = {}

                if hit.diagmessage not in harvester_q[hit.computingsite]['errors']:
                    harvester_q[hit.computingsite]['errors'][hit.diagmessage] = 1
                else:
                    harvester_q[hit.computingsite]['errors'][hit.diagmessage] += 1

                if hit.diagmessage not in harvester_ce[hit.computingsite][hit.computingelement]['errors']:
                    harvester_ce[hit.computingsite][hit.computingelement]['errors'][hit.diagmessage] = 1
                else:
                    harvester_ce[hit.computingsite][hit.computingelement]['errors'][hit.diagmessage] += 1

                if hit.diagmessage not in harvester_schedd_ce[submissionhost][hit.computingelement]['errors']:
                    harvester_schedd_ce[submissionhost][hit.computingelement]['errors'][hit.diagmessage] = 1
                else:
                    harvester_schedd_ce[submissionhost][hit.computingelement]['errors'][hit.diagmessage] += 1

            if hit.status in ('finished'):
                harvester_q[hit.computingsite]['totalworkers'] += 1
                harvester_q[hit.computingsite]['goodworkers'] += 1

                harvester_ce[hit.computingsite][hit.computingelement]['totalworkers'] += 1
                harvester_ce[hit.computingsite][hit.computingelement]['goodworkers'] += 1

                harvester_schedd_ce[submissionhost][hit.computingelement]['totalworkers'] += 1
                harvester_schedd_ce[submissionhost][hit.computingelement]['goodworkers'] += 1


        return harvester_q, harvester_ce, harvester_schedd_ce