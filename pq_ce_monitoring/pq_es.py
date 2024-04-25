from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from opensearchpy import Search, Q

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

        s.aggs.bucket('computingsite', 'terms', field='computingsite.keyword', size=10000)
        s.aggs['computingsite'].bucket('workerstats', 'filters',
                                       filters={
                                           'good': Q('terms', status=['finished']),
                                           'bad': Q('terms', status=['cancelled', 'failed', 'missed'])
                                       }
                                       )
        s.aggs['computingsite']['workerstats'].bucket('errors', 'terms', field='diagmessage.keyword', size=10000)

        s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=10000)
        s.aggs['computingelement'].bucket('workerstats', 'filters',
                                          filters={
                                              'good': Q('terms', status=['finished']),
                                              'bad': Q('terms', status=['cancelled', 'failed', 'missed'])
                                          }
                                          )
        s.aggs['computingelement']['workerstats'].bucket('errors', 'terms', field='diagmessage.keyword', size=10000)

        s = s.execute()
        return s

    def get_workers_info(self, tdelta, time='submittime'):
        """
        Get workers info via SCAN from OpenSearch cluster
        :param tdelta: time interval. Default value is 60 minutes
        :param time: type of timestamp. Default value is submittime
        :return: PQ dict, CE dict, Data frame for clusterlogs
        """
        connection = self.connection

        date_to = datetime.utcnow()
        date_from = date_to - timedelta(minutes=tdelta)
        date_to = date_to.strftime("%Y-%m-%dT%H:%M")[:-2] + '00:00'
        date_from = date_from.strftime("%Y-%m-%dT%H:%M")[:-2] + '00:00'
        #genes_filter = Q('bool', must=[Q('terms', status=['failed', 'finished', 'canceled', 'missed'])])
        s = Search(using=connection, index='atlas_harvesterworkers-*')
        s = s.filter('range', **{time: {'gte': date_from,
                                        'lt': date_to}})
        s = s.source(['diagmessage', 'meta', 'status', 'computingelement', 'computingsite'])
        response = s.scan()

        harvester_q = {}
        harvester_ce = {}
        errors_list = []

        for worker in response:
            if worker.computingsite not in harvester_q:
                harvester_q[worker.computingsite] = {'totalworkers': 0, 'badworkers': 0, 'goodworkers': 0}

            if worker.computingsite not in harvester_ce:
                harvester_ce[worker.computingsite] = {}
                if worker.computingelement not in harvester_ce[worker.computingsite]:
                    harvester_ce[worker.computingsite][worker.computingelement] = {'totalworkers': 0, 'badworkers': 0,
                                                                             'goodworkers': 0}
            else:
                if worker.computingelement not in harvester_ce[worker.computingsite]:
                    harvester_ce[worker.computingsite][worker.computingelement] = {'totalworkers': 0, 'badworkers': 0,
                                                                             'goodworkers': 0}

            if worker.status in ('failed', 'missed', 'cancelled'):
                harvester_q[worker.computingsite]['badworkers'] += 1
                harvester_q[worker.computingsite]['totalworkers'] += 1

                harvester_ce[worker.computingsite][worker.computingelement]['totalworkers'] += 1
                harvester_ce[worker.computingsite][worker.computingelement]['badworkers'] += 1

                if 'errors' not in harvester_q[worker.computingsite]:
                    harvester_q[worker.computingsite]['errors'] = {}
                if worker.diagmessage not in harvester_q[worker.computingsite]['errors']:
                    harvester_q[worker.computingsite]['errors'][worker.diagmessage] = 1
                else:
                    harvester_q[worker.computingsite]['errors'][worker.diagmessage] += 1

                if 'errors' not in harvester_ce[worker.computingsite][worker.computingelement]:
                    harvester_ce[worker.computingsite][worker.computingelement]['errors'] = {}
                if worker.diagmessage not in harvester_ce[worker.computingsite][worker.computingelement]['errors']:
                    harvester_ce[worker.computingsite][worker.computingelement]['errors'][worker.diagmessage] = 1
                else:
                    harvester_ce[worker.computingsite][worker.computingelement]['errors'][worker.diagmessage] += 1
                ### Cluster logs ###
                errors_list.append([worker.meta.id, worker.diagmessage])

            if worker.status in ('finished'):
                harvester_q[worker.computingsite]['totalworkers'] += 1
                harvester_q[worker.computingsite]['goodworkers'] += 1

                harvester_ce[worker.computingsite][worker.computingelement]['totalworkers'] += 1
                harvester_ce[worker.computingsite][worker.computingelement]['goodworkers'] += 1
        ### Cluster logs ###
        errors_df = pd.DataFrame(errors_list, columns=['id', 'message'])

        return harvester_q, harvester_ce, errors_df

    def get_stuck_ces(self, computingelement_list):

        time_list = ['now-6h', 'now-12h', 'now-24h']

        ces_candidats = {}

        first_iteration = True

        connection = self.connection

        for gte in time_list:

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lt': 'now'}})

            s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=10000)
            s.aggs['computingelement'].bucket('computingsites', 'terms', field='computingsite.keyword', size=10000)
            s.aggs['computingelement']['computingsites'].bucket('workersstatus', 'terms', field='status.keyword',
                                                                size=10000)

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
                     'now-6h', 'now-12h', 'now-1d', 'now-7d', 'now-30d']

        ces_candidats_avg = {}
        ces_candidats_count = {}

        connection = self.connection

        for gte in time_list:

            n_workers_avg = {}
            n_workers_count = {}

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lte': 'now'}})
            s.aggs.bucket('computingelement', 'terms', field='computingelement.keyword', size=10000)
            s.aggs['computingelement'].bucket('computingelement_per_hour', 'date_histogram', field='submittime',
                                              interval='1h') \
                .metric('computingelement_count', 'value_count', field='computingelement.keyword') \
                .metric('number_workers', 'sum', field='ncore') \
                .pipeline('moving_avg', 'moving_avg', buckets_path='computingelement_count') \
                .pipeline('moving_avg_w', 'moving_avg', window=100, buckets_path='number_workers')

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

        ceslist = ces_candidats_count

        for ce in ceslist:
            first_not_zero = 0
            date_for_analysis = 'now-7d'

            if ce != 'none':
                if date_for_analysis in ceslist[ce] and ceslist[ce][date_for_analysis] != 0 and first_not_zero == 0:
                    first_not_zero = ceslist[ce][date_for_analysis]
                else:
                    ces_candidats.add(ce)
                    continue
                # if ces_candidats_avg[ce]['now-12h'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-12h']
                # if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-1d']
                # if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-1d']
                # if ces_candidats_avg[ce]['now-2d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-2d']
                # if ces_candidats_avg[ce]['now-3d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-3d']
                if 'now-7d' in ceslist[ce] and ceslist[ce]['now-7d'] != 0 and first_not_zero == 0:
                     first_not_zero = ceslist[ce]['now-7d']
                if 'now-30d' in ceslist[ce] and ceslist[ce]['now-30d'] != 0 and first_not_zero == 0:
                    first_not_zero = ceslist[ce]['now-30d']
                for time in ceslist[ce]:
                    try:
                        if time == date_for_analysis:
                            break
                        if time not in ['now-1h', 'now-2h', 'now-30d']:
                            if int((ceslist[ce][time]/first_not_zero)*100) < lim:
                                ces_candidats.add(ce)
                                break
                    except:
                        ces_candidats.add(ce)
        for ce in list(ceslist):
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
            s.aggs.bucket('computingsite', 'terms', field='computingsite.keyword', size=10000)
            s.aggs['computingsite'].bucket('computingsite_per_hour', 'date_histogram', field='submittime',
                                              interval='1h') \
                .metric('computingsite_count', 'value_count', field='computingsite.keyword') \
                .metric('number_workers', 'sum', field='ncore') \
                .pipeline('moving_avg', 'moving_avg', buckets_path='computingsite_count') \
                .pipeline('moving_avg_w', 'moving_avg', window=100, buckets_path='number_workers')

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

        lim = 40
        pq_candidats = set()

        pqlist = pq_candidats_count

        for ce in pqlist:
            first_not_zero = 0
            date_for_analysis = 'now-7d'

            if ce != 'none':
                if date_for_analysis in pqlist[ce] and pqlist[ce][date_for_analysis] != 0 and first_not_zero == 0:
                    first_not_zero = pqlist[ce][date_for_analysis]
                else:
                    pq_candidats.add(ce)
                    continue
                # if ces_candidats_avg[ce]['now-12h'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-12h']
                # if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-1d']
                # if ces_candidats_avg[ce]['now-1d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-1d']
                # if ces_candidats_avg[ce]['now-2d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-2d']
                # if ces_candidats_avg[ce]['now-3d'] != 0 and first_not_zero == 0:
                #     first_not_zero = ces_candidats_avg[ce]['now-3d']
                if 'now-7d' in pqlist[ce] and pqlist[ce]['now-7d'] != 0 and first_not_zero == 0:
                    first_not_zero = pqlist[ce]['now-7d']
                if 'now-30d' in pqlist[ce] and pqlist[ce]['now-30d'] != 0 and first_not_zero == 0:
                    first_not_zero = pqlist[ce]['now-30d']
                for time in pqlist[ce]:
                    try:
                        if time == date_for_analysis:
                            break
                        if time not in ['now-1h', 'now-2h', 'now-30d']:
                            if int((pqlist[ce][time] / first_not_zero) * 100) < lim:
                                pq_candidats.add(ce)
                                break
                    except:
                        pq_candidats.add(ce)
        for ce in list(pqlist):
            if ce not in pq_candidats:
                del pq_candidats_avg[ce]
                del pq_candidats_count[ce]
        return pq_candidats_avg, pq_candidats_count

    def get_inactive_elements(self):
        time_list = ['now-1h', 'now-2h', 'now-3h', 'now-4h', 'now-5h',
                     'now-6h', 'now-12h', 'now-1d', 'now-7d', 'now-30d']

        inactive_elements = {}

        connection = self.connection

        for gte in time_list:
            print(gte)
            n_workers_count = {}

            s = Search(using=connection, index='atlas_harvesterworkers-*')
            s = s.filter('range', **{'submittime': {'gte': gte,
                                                    'lte': 'now'}})
            s.aggs.bucket('computingsite', 'terms', field='computingsite.keyword')
            s.aggs['computingsite'].bucket('computingelement', 'terms', field='computingelement.keyword')
            s.aggs['computingsite']['computingelement'].bucket('computingelement_per_hour', 'date_histogram',
                                                               field='submittime',
                                              interval='1h') \
                .metric('computingelement_count', 'value_count', field='computingelement.keyword') \
                .metric('number_workers', 'sum', field='ncore')

            response = s.execute()

            for computingsite in response.aggregations.computingsite:
                n_workers_count[computingsite.key] = {}
                for computingelement in computingsite.computingelement:
                    n_workers_count[computingsite.key][computingelement.key] = []
                    for pq in computingelement.computingelement_per_hour:
                        nworkers = pq['number_workers']['value']
                        n_workers_count[computingsite.key][computingelement.key].append(nworkers)
                    n_workers_count[computingsite.key][computingelement.key] \
                        = np.mean(n_workers_count[computingsite.key][computingelement.key])

            for pq in n_workers_count.keys():
                if pq not in inactive_elements:
                    inactive_elements[pq] = {}
                for ce in n_workers_count[pq].keys():
                    if ce not in inactive_elements[pq]:
                        inactive_elements[pq][ce] = {}
                        inactive_elements[pq][ce][gte] = n_workers_count[pq][ce]
                    else:
                        inactive_elements[pq][ce][gte] = n_workers_count[pq][ce]
            s = None
        inactive_candidats = set()

        inList = inactive_elements
        for pq in inList.keys():
            for ce in inList[pq].keys():
                first_not_zero = 0
                date_for_analysis = 'now-7d'

                if ce != 'none':
                    if date_for_analysis in inList[pq][ce] and inList[pq][ce][date_for_analysis] != 0 \
                            and first_not_zero == 0:
                        first_not_zero = inList[pq][ce][date_for_analysis]
                    else:
                        inactive_candidats.add(pq + '|' + ce)
                        continue
                    if 'now-7d' in inList[pq][ce] and inList[pq][ce]['now-7d'] != 0 and first_not_zero == 0:
                        first_not_zero = inList[pq][ce]['now-7d']
                    if 'now-30d' in inList[pq][ce] and inList[pq][ce]['now-30d'] != 0 and first_not_zero == 0:
                        first_not_zero = inList[pq][ce]['now-30d']
                    for time in inList[pq][ce]:
                        try:
                            if time == date_for_analysis:
                                break
                            if time not in ['now-30d']:
                                if int(round(inList[pq][ce][time])) == 0:
                                    inactive_candidats.add(pq + '|' + ce)
                                    break
                        except:
                            inactive_candidats.add(pq + '|' + ce)
        for pq in list(inList.keys()):
            for ce in list(inList[pq]):
                if pq+'|'+ce not in inactive_candidats:
                    del inactive_elements[pq][ce]
            if len(inactive_elements[pq]) == 0:
                 del inactive_elements[pq]

        return inactive_elements