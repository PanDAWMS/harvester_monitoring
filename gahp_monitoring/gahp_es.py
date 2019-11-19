from elasticsearch_dsl import Search, Q
from datetime import datetime, timedelta

from baseclasses.esbaseclass import EsBaseClass

from logger import ServiceLogger

_logger = ServiceLogger("es_gahp", __file__).logger


class GahpMonitoringEs(EsBaseClass):

    def __init__(self, path):
        super().__init__(path)

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