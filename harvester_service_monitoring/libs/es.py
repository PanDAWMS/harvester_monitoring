from elasticsearch_dsl import Search
from datetime import datetime
from logger import ServiceLogger
from baseclasses.esbaseclass import EsBaseClass

_logger = ServiceLogger("es", __file__).logger


class Es(EsBaseClass):

    def __init__(self, path):
        super().__init__(path)

    def get_workers_stats(self):
        """
        Get workers stats for harvester hosts
        """
        connection = self.connection

        s = Search(using=connection, index='atlas_harvesterworkers-*')[:0]

        #s = s.exclude('terms', status=['missed'])

        s.aggs.bucket('harvesterid', 'terms', field='harvesterid.keyword', size=1000) \
            .metric('max_submittime', 'max', field='submittime') \
            .metric('min_submittime', 'min', field='submittime') \
            .bucket('harvesterhost', 'terms', field='harvesterhost.keyword', order={'max_hostsubmittime': 'desc'},
                    size=100) \
            .metric('max_hostsubmittime', 'max', field='submittime')

        s = s.execute()

        harvesteridDict = {}

        worker_statuses = self.get_last_worker_statuses()

        for harvesterid in s.aggregations.harvesterid:
            harvesteridDict[harvesterid.key] = {
                'max': datetime.strptime(harvesterid.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z'),
                'min': datetime.strptime(harvesterid.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z')
            }
            if len(harvesterid.harvesterhost) == 0:
                harvesteridDict[harvesterid.key]['harvesterhost'] = {}
                harvesteridDict[harvesterid.key]['harvesterhost']['none'] = {
                    'harvesterhostmaxtime': datetime.strptime(harvesterid.max_submittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z'),
                    'harvesterhostmintime': datetime.strptime(harvesterid.max_submittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z')}
            for harvesterhost in harvesterid.harvesterhost:
                if 'harvesterhost' not in harvesteridDict[harvesterid.key]:
                    harvesteridDict[harvesterid.key]['harvesterhost'] = {}
                if harvesterid.key in worker_statuses:
                    if harvesterhost.key in worker_statuses[harvesterid.key]['harvesterhost']:
                        wstats = worker_statuses[harvesterid.key]['harvesterhost'][harvesterhost.key]['wstats']
                    else:
                        wstats = 'None'
                else:
                    wstats = 'None'
                harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key] = {
                    'harvesterhostmaxtime': datetime.strptime(harvesterhost.max_hostsubmittime.value_as_string,
                                                              '%Y-%m-%dT%H:%M:%S.000Z'),
                    'wstats': wstats

                }
        return harvesteridDict

    def get_last_worker_statuses(self):
        """
        Get last harvester workers statuses
        """
        connection = self.connection

        s = Search(using=self.connection, index='atlas_harvesterworkers-*').filter('range', **{
            '@timestamp': {'gte': 'now-30m', 'lte': 'now'}})

        s.aggs.bucket('harvesterid', 'terms', field='harvesterid.keyword', size=1000) \
            .bucket('harvesterhost', 'terms', field='harvesterhost.keyword', size=1000) \
            .bucket('workerstatuses', 'terms', field='status.keyword', size=1000)

        s = s.execute()

        harvesteridDict = {}

        for harvesterid in s.aggregations.harvesterid:
            if harvesterid.key not in harvesteridDict:
                harvesteridDict[harvesterid.key] = {}
            if len(harvesterid.harvesterhost) == 0:
                harvesteridDict[harvesterid.key]['harvesterhost'] = {}
                harvesteridDict[harvesterid.key]['harvesterhost']['none'] = {}
            for harvesterhost in harvesterid.harvesterhost:
                if 'harvesterhost' not in harvesteridDict[harvesterid.key]:
                    harvesteridDict[harvesterid.key]['harvesterhost'] = {}
                harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key] = {}
                harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key]['wstats'] = {}
                if 'total_workers' not in harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key]['wstats']:
                    harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key]['wstats']['total_workers'] = harvesterhost.doc_count
                for status in harvesterhost.workerstatuses:
                     harvesteridDict[harvesterid.key]['harvesterhost'][harvesterhost.key]['wstats'][status.key] = status.doc_count

        return harvesteridDict


    def get_harvester_exceptions(self):
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
            .metric('last_entry', 'top_hits', size=1, _source=['creation_time', 'condor_collector', 'condor_negotiator',
                                                               'condor_schedd', 'disk_usage_cephfs', 'disk_usage_data',
                                                               'disk_usage_data1', 'disk_usage_data1',
                                                               'disk_usage_data2',
                                                               'n_workers_created_total'
                                                               ], sort=[{'creation_time': 'desc'}]) \
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
                submissionhostDict[key]['last_submittime'] = datetime.strptime(hit.max_submittime.value_as_string,
                                                                               '%Y-%m-%dT%H:%M:%S.000Z')
        return submissionhostDict