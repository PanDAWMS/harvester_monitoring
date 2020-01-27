from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from elasticsearch_dsl import Search, Q

from baseclasses.esbaseclass import EsBaseClass
from logger import ServiceLogger

_logger = ServiceLogger("pq_es", __file__).logger


class PandaQEs(EsBaseClass):

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

    def get_info_workers(self, type, tdelta=60, time='submittime'):

        connection = self.connection

        date_UTC = datetime.utcnow()
        date_str = date_UTC - timedelta(minutes=tdelta)
        genes_filter = Q('bool', must=[Q('terms', status=['failed', 'finished', 'canceled', 'missed'])])
        s = Search(using=connection, index='atlas_harvesterworkers-*')
        s = s.filter('range', **{time: {'gte': date_str.strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00',
                                        'lt': datetime.utcnow().strftime("%Y-%m-%dT%H:%M")[:-1] + '0:00'}})

        response = s.scan()

        harvester_q = {}
        harvester_ce = {}
        harvester_schedd_ce = {}

        for hit in response:
            if type == 'ce_pq':
                if hit.computingsite not in harvester_q:
                    harvester_q[hit.computingsite] = {'totalworkers': 0, 'badworkers': 0, 'goodworkers': 0}

                if hit.computingsite not in harvester_ce:
                    harvester_ce[hit.computingsite] = {}
                    if hit.computingelement not in harvester_ce[hit.computingsite]:
                        harvester_ce[hit.computingsite][hit.computingelement] = {'totalworkers': 0, 'badworkers': 0,
                                                                                 'goodworkers': 0}
                else:
                    if hit.computingelement not in harvester_ce[hit.computingsite]:
                        harvester_ce[hit.computingsite][hit.computingelement] = {'totalworkers': 0, 'badworkers': 0,
                                                                                 'goodworkers': 0}
            elif type == 'gahp':
                submissionhost = hit.submissionhost.split(',')[0]
                if hit.computingelement not in harvester_schedd_ce:
                    harvester_schedd_ce[hit.computingelement] = {}
                    if submissionhost not in harvester_schedd_ce[hit.computingelement]:
                        harvester_schedd_ce[hit.computingelement][submissionhost] = {'totalworkers': 0, 'badworkers': 0,
                                                                                     'goodworkers': 0,
                                                                                     'computingsite': hit.computingsite}
                else:
                    if submissionhost not in harvester_schedd_ce[hit.computingelement]:
                        harvester_schedd_ce[hit.computingelement][submissionhost] = {'totalworkers': 0, 'badworkers': 0,
                                                                                     'goodworkers': 0,
                                                                                     'computingsite': hit.computingsite}

            if hit.status in ('failed', 'missed', 'cancelled'):
                if type == 'ce_pq':
                    harvester_q[hit.computingsite]['badworkers'] += 1
                    harvester_q[hit.computingsite]['totalworkers'] += 1

                    harvester_ce[hit.computingsite][hit.computingelement]['totalworkers'] += 1
                    harvester_ce[hit.computingsite][hit.computingelement]['badworkers'] += 1

                    if 'errors' not in harvester_q[hit.computingsite]:
                        harvester_q[hit.computingsite]['errors'] = {}

                    if 'errors' not in harvester_ce[hit.computingsite][hit.computingelement]:
                        harvester_ce[hit.computingsite][hit.computingelement]['errors'] = {}

                    if hit.diagmessage not in harvester_q[hit.computingsite]['errors']:
                        harvester_q[hit.computingsite]['errors'][hit.diagmessage] = 1
                    else:
                        harvester_q[hit.computingsite]['errors'][hit.diagmessage] += 1

                    if hit.diagmessage not in harvester_ce[hit.computingsite][hit.computingelement]['errors']:
                        harvester_ce[hit.computingsite][hit.computingelement]['errors'][hit.diagmessage] = 1
                    else:
                        harvester_ce[hit.computingsite][hit.computingelement]['errors'][hit.diagmessage] += 1
                elif type == 'gahp':
                    harvester_schedd_ce[hit.computingelement][submissionhost]['totalworkers'] += 1
                    harvester_schedd_ce[hit.computingelement][submissionhost]['badworkers'] += 1
                    if 'errors' not in harvester_schedd_ce[hit.computingelement][submissionhost]:
                        harvester_schedd_ce[hit.computingelement][submissionhost]['errors'] = {}
                    if hit.diagmessage not in harvester_schedd_ce[hit.computingelement][submissionhost]['errors']:
                        harvester_schedd_ce[hit.computingelement][submissionhost]['errors'][hit.diagmessage] = 1
                    else:
                        harvester_schedd_ce[hit.computingelement][submissionhost]['errors'][hit.diagmessage] += 1

            if hit.status in ('finished') and type == 'ce_pq':
                harvester_q[hit.computingsite]['totalworkers'] += 1
                harvester_q[hit.computingsite]['goodworkers'] += 1

                harvester_ce[hit.computingsite][hit.computingelement]['totalworkers'] += 1
                harvester_ce[hit.computingsite][hit.computingelement]['goodworkers'] += 1

            if hit.status in ('finished', 'submitted', 'running') and type == 'gahp':
                harvester_schedd_ce[hit.computingelement][submissionhost]['totalworkers'] += 1
                harvester_schedd_ce[hit.computingelement][submissionhost]['goodworkers'] += 1

        if type == 'ce_pq':
            return harvester_q, harvester_ce
        elif type == 'gahp':
            return harvester_schedd_ce

    def get_stuck_ces(self, computingelement_list):

        time_list = ['now-6h', 'now-12h', 'now-24h']

        ces_candidats = {}

        first_iteration = True

        connection = self.connection

        for gte in time_list:

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lt': 'now'}})

            s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=50000)
            s.aggs['computingelement'].bucket('computingsites', 'terms', field='computingsite.keyword', size=50000)
            s.aggs['computingelement']['computingsites'].bucket('workersstatus', 'terms', field='status.keyword',
                                                                size=50000)

            s = s.execute()

            if first_iteration:
                for computingelement in s.aggregations.computingelement:
                    for computingsite in computingelement.computingsites:
                        if len(computingsite.workersstatus) == 1:
                            for statuses in computingsite.workersstatus:
                                if statuses.key in ('submitted', 'cancelled'):
                                    ce_c = computingelement.key
                                    if '//' in ce_c:
                                        ce_c = str(ce_c).split('//')[-1]
                                    if ':' in ce_c:
                                        ce_c = str(ce_c).split(':')[0]
                                    if ce_c not in ces_candidats:
                                        ces_candidats[ce_c] = {}
                                    if computingsite.key not in ces_candidats[ce_c]:
                                        ces_candidats[ce_c][computingsite.key] = {'workerstatuses':
                                                                                      computingsite.workersstatus.buckets,
                                                                                  }
                first_iteration = False
            else:
                ces_list = []
                for computingelement in s.aggregations.computingelement:
                    ce_c = computingelement.key
                    if '//' in ce_c:
                        ce_c = str(ce_c).split('//')[-1]
                    if ':' in ce_c:
                        ce_c = str(ce_c).split(':')[0]
                    ces_list.append(ce_c)
                    for computingsite in computingelement.computingsites:
                        for statuses in computingsite.workersstatus:
                            if statuses.key not in ('submitted', 'cancelled') and ce_c in ces_candidats \
                                    and computingsite.key in ces_candidats[ce_c]:
                                del ces_candidats[ce_c][computingsite.key]
                                # break
                if gte == 'now-12h':
                    for ce in computingelement_list:
                        if ce not in ces_list:
                            ces_candidats[ce] = {}
                            for computingsite in computingelement_list[ce]:
                                ces_candidats[ce][computingsite] = {}
        return ces_candidats

    def get_suspicious_ces(self):

        time_list = ['now-1h', 'now-2h', 'now-3h', 'now-4h', 'now-5h',
                     'now-6h', 'now-12h', 'now-1d', 'now-2d', 'now-3d', 'now-7d', 'now-30d']

        ces_candidats_avg = {}
        ces_candidats_count = {}

        connection = self.connection

        for gte in time_list:

            n_workers_avg = {}
            n_workers_count = {}

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lte': 'now'}})
            s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=1000000)
            s.aggs['computingelement'].bucket('computingelement_per_hour', 'date_histogram', field='submittime',
                                              interval='1h') \
                .metric('computingelement_count', 'value_count', field='computingelement.keyword') \
                .metric('number_workers', 'sum', field='ncore') \
                .pipeline('moving_avg', 'moving_avg', buckets_path='computingelement_count') \
                .pipeline('moving_avg_w', 'moving_avg', buckets_path='number_workers')
            s = s.execute()

            for hit in s.aggregations.computingelement:
                n_workers_count[hit.key] = []
                n_workers_avg[hit.key] = []
                for ce in hit.computingelement_per_hour:
                   nworkers = ce['number_workers']['value']
                   #nworkers = ce.doc_count
                   if 'moving_avg' in ce:
                        avg = ce['moving_avg_w']['value']
                        n_workers_avg[hit.key].append(avg)
                   n_workers_count[hit.key].append(nworkers)
                # del n_workers_count[hit.key][0]
                n_workers_count[hit.key] = np.mean(n_workers_count[hit.key])
                n_workers_avg[hit.key] = np.mean(n_workers_avg[hit.key])

            for ce in n_workers_avg.keys():
                if ce not in ces_candidats_avg:
                    ces_candidats_avg[ce] = {}
                    ces_candidats_avg[ce][gte] = n_workers_avg[ce]
                else:
                    ces_candidats_avg[ce][gte] = n_workers_avg[ce]

            for ce in n_workers_count.keys():
                if ce not in ces_candidats_count:
                    ces_candidats_count[ce] = {}
                    ces_candidats_count[ce][gte] = n_workers_count[ce]
                else:
                    ces_candidats_count[ce][gte] = n_workers_count[ce]
        df_avg = pd.DataFrame.from_dict(ces_candidats_avg, orient='index')
        df_count = pd.DataFrame.from_dict(ces_candidats_count, orient='index')

        lim = 40
        ces_candidats = set()


        for ce in ces_candidats_avg:
            first_not_zero = 0

            if ce != 'none':
                if 'now-6h' in ces_candidats_avg[ce] and ces_candidats_avg[ce]['now-6h'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-6h']
                else:
                    ces_candidats.add(ce)
                    continue
                if ces_candidats_avg[ce]['now-12h'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-12h']
                if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-1d']
                if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-1d']
                if ces_candidats_avg[ce]['now-2d'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-2d']
                if ces_candidats_avg[ce]['now-3d'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-3d']
                if ces_candidats_avg[ce]['now-7d'] != 0 and first_not_zero == 0:
                    first_not_zero = ces_candidats_avg[ce]['now-7d']
                for time in ces_candidats_avg[ce]:
                    try:
                        if time not in ['now-1h', 'now-2h', 'now-3h']:
                            if int((ces_candidats_avg[ce][time]/first_not_zero)*100) < lim:
                                ces_candidats.add(ce)
                                break
                    except:
                        ces_candidats.add(ce)
        for ce in list(ces_candidats_avg):
            if ce not in ces_candidats:
                del ces_candidats_avg[ce]
                del ces_candidats_count[ce]
        return ces_candidats_avg, ces_candidats_count

    def get_suspicious_pq(self):
        time_list = ['now-1h', 'now-2h', 'now-3h', 'now-4h', 'now-5h',
                     'now-6h', 'now-12h', 'now-1d', 'now-7d', 'now-30d']

        pq_candidats_avg = {}
        pq_candidats_count = {}

        connection = self.connection

        for gte in time_list:

            n_workers_avg = {}
            n_workers_count = {}

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lte': 'now'}})
            s.aggs.bucket('computingsite', 'terms', field='computingsite.keyword', size=1000000)
            s.aggs['computingsite'].bucket('computingsite_per_hour', 'date_histogram', field='submittime',
                                              interval='1h') \
                .metric('computingsite_count', 'value_count', field='computingsite.keyword') \
                .metric('number_workers', 'sum', field='ncore') \
                .pipeline('moving_avg', 'moving_avg', buckets_path='computingsite_count') \
                .pipeline('moving_avg_w', 'moving_avg', buckets_path='number_workers')

            s = s.execute()

            for hit in s.aggregations.computingsite:
                n_workers_count[hit.key] = []
                n_workers_avg[hit.key] = []
                for pq in hit.computingsite_per_hour:
                   nworkers = pq['number_workers']['value']
                   #nworkers = ce.doc_count
                   if 'moving_avg' in pq:
                        avg = pq['moving_avg_w']['value']
                        n_workers_avg[hit.key].append(avg)
                   n_workers_count[hit.key].append(nworkers)
                # del n_workers_count[hit.key][0]
                n_workers_count[hit.key] = np.mean(n_workers_count[hit.key])
                n_workers_avg[hit.key] = np.mean(n_workers_avg[hit.key])

            for pq in n_workers_avg.keys():
                if pq not in pq_candidats_avg:
                    pq_candidats_avg[pq] = {}
                    pq_candidats_avg[pq][gte] = n_workers_avg[pq]
                else:
                    pq_candidats_avg[pq][gte] = n_workers_avg[pq]

            for pq in n_workers_count.keys():
                if pq not in pq_candidats_count:
                    pq_candidats_count[pq] = {}
                    pq_candidats_count[pq][gte] = n_workers_count[pq]
                else:
                    pq_candidats_count[pq][gte] = n_workers_count[pq]
        df_avg = pd.DataFrame.from_dict(pq_candidats_avg, orient='index')
        df_count = pd.DataFrame.from_dict(pq_candidats_count, orient='index')
        return pq_candidats_avg, pq_candidats_count