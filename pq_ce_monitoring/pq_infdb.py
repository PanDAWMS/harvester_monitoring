import requests
import time

from datetime import datetime

from . pq_es import PandaQEs
from . pq_pandadb import PandaDBPQ

from baseclasses.infdbbaseclass import InfluxDbBaseClass
from accounting.error_accounting import Errors

from logger import ServiceLogger

_logger = ServiceLogger("pq_influxdb", __file__).logger


class InfluxPQ(InfluxDbBaseClass):
    def __init__(self, path):
        super().__init__(path)

    def write_data_backup(self, tdelta):

        date_key = datetime.now()

        es = PandaQEs(self.path)

        s = es.get_ratio(tdelta)

        harvester_queues = {}
        harvester_queues_errors = {}

        errors_object = Errors('patterns.txt')

        for hit in s.aggregations.computingsite:
            harvester_queues[hit.key] = {'totalworkers': hit.doc_count,
                                         'goodworkers': hit.workerstats.buckets['good']['doc_count'],
                                         'badworkers': hit.workerstats.buckets['bad']['doc_count'],
                                         # 'ratio': ((float(hit.workerstats.buckets['bad']['doc_count']) / float(
                                         #     hit.doc_count))) * 100}
                                         }
            if int(hit.workerstats.buckets['bad']['doc_count']) > 0:
                harvester_queues_errors = errors_object.accounting(hit, harvester_queues_errors)

        harvester_computingelements = {}
        harvester_computingelements_errors = {}

        for hit in s.aggregations.computingelement:
            harvester_computingelements[hit.key] = {'totalworkers': hit.doc_count,
                                                    'goodworkers': hit.workerstats.buckets['good']['doc_count'],
                                                    'badworkers': hit.workerstats.buckets['bad']['doc_count'],
                                                    # 'ratio': ((float(hit.workerstats.buckets['bad']['doc_count']) / float(
                                                    #     hit.doc_count))) * 100}
                                                    }
            if int(hit.workerstats.buckets['bad']['doc_count']) > 0:
                harvester_computingelements_errors = errors_object.accounting(hit, harvester_computingelements_errors)

        q_keys = list(harvester_queues.keys())
        q_errors_keys = list(harvester_queues_errors.keys())

        harvester_queues_json_influxdb = []
        harvester_queues_errors_json_influxdb = []

        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1] + '0:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        for queuename, queue in queues.items():
            if queuename in q_keys:
                harvester_queues_json_influxdb.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester']
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": harvester_queues[queuename]['totalworkers'],
                            "goodworkers": harvester_queues[queuename]['goodworkers'],
                            "badworkers": harvester_queues[queuename]['badworkers'],
                        }
                    }
                )
                if queuename in q_errors_keys:
                    for error in harvester_queues_errors[queuename]:
                        harvester_queues_errors_json_influxdb.append(
                            {
                                "measurement": "computingsite_errors",
                                "tags": {
                                    "computingsite": queuename,
                                    "cloud": queue['cloud'],
                                    "atlas_site": queue['atlas_site'],
                                    "errordesc": error,
                                    "harvester": queue['harvester'],
                                    "status": queue['status']
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_queues[queuename]['totalworkers'],
                                    "goodworkers": harvester_queues[queuename]['goodworkers'],
                                    "badworkers": harvester_queues[queuename]['badworkers'],
                                    # "ratio": float(harvester_queues[queuename]['ratio']),
                                    "error_count": harvester_queues_errors[queuename][error]['error_count'],
                                    "total_error_count": harvester_queues_errors[queuename][error]['total_error_count'],
                                }
                            }
                        )
            else:
                harvester_queues_json_influxdb.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester']
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": 0,
                            "goodworkers": 0,
                            "badworkers": 0,
                            "ratio": 0.00
                        }
                    }
                )

        harvester_ce_json_influxdb = []
        harvester_ce_errors_json_influxdb = []

        ce_errors_keys = list(harvester_computingelements_errors.keys())

        for ce in harvester_computingelements:
            harvester_ce_json_influxdb.append(
                {
                    "measurement": "computingelements",
                    "tags": {
                        "computingelement": ce
                    },
                    "time": int(time_stamp),
                    "fields": {
                        "totalworkers": harvester_computingelements[ce]['totalworkers'],
                        "goodworkers": harvester_computingelements[ce]['goodworkers'],
                        "badworkers": harvester_computingelements[ce]['badworkers'],
                    }
                }
            )
            if ce in ce_errors_keys:
                for error in harvester_computingelements_errors[ce]:
                    harvester_ce_errors_json_influxdb.append(
                        {
                            "measurement": "computingelement_errors",
                            "tags": {
                                "computingelement": ce,
                                "errordesc": error,
                            },
                            "time": int(time_stamp),
                            "fields": {
                                "totalworkers": harvester_computingelements[ce]['totalworkers'],
                                "goodworkers": harvester_computingelements[ce]['goodworkers'],
                                "badworkers": harvester_computingelements[ce]['badworkers'],
                                # "ratio": float(harvester_computingelements[ce]['ratio']),
                                "error_count": harvester_computingelements_errors[ce][error]['error_count'],
                                "total_error_count": harvester_computingelements_errors[ce][error]['total_error_count'],
                            }
                        }
                    )

        try:
            self.connection.write_points(harvester_queues_json_influxdb, time_precision='s')
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_queues_errors_json_influxdb, time_precision='s')
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_json_influxdb, time_precision='s')
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_errors_json_influxdb, time_precision='s')
        except Exception as ex:
            _logger.error(ex)

    def write_data(self, tdelta=60):

        date_key = datetime.now()

        es = PandaQEs(self.path)

        harvester_queues, harvester_computingelements, errors_df = es.get_workers_info(tdelta=tdelta, time='endtime')
        errors_object = Errors('patterns.txt')
        errors_object_cl = Errors('patterns_cl.txt')
        errors_object_cl_ml = Errors('patterns_cl_ml.txt')

        if len(errors_df) != 0:
            errors_object_cl.write_patterns(errors_df)
            errors_object_cl_ml.write_patterns(errors_df, clustering_type='ML', mode='update', model_name='harvester_30days.model')

        harvester_queues_errors = {}
        harvester_computingelements_errors = {}

        harvester_queues_errors_cl = {}
        harvester_computingelements_errors_cl = {}

        harvester_queues_errors_cl_ml = {}
        harvester_computingelements_errors_cl_ml = {}

        for queuename in harvester_queues:
            if 'errors' in harvester_queues[queuename]:
                harvester_queues_errors = errors_object.errors_accounting(queuename,
                                                                          harvester_queues[queuename]['errors'],
                                                                          harvester_queues_errors,
                                                                          harvester_queues[queuename]['badworkers'])
                # Cluster logs #
                harvester_queues_errors_cl = errors_object_cl.errors_accounting(queuename,
                                                                                harvester_queues[queuename]['errors'],
                                                                                harvester_queues_errors_cl,
                                                                                harvester_queues[queuename]['badworkers'])
                # Cluster logs ML#
                harvester_queues_errors_cl_ml = errors_object_cl_ml.errors_accounting(queuename,
                                                                                harvester_queues[queuename]['errors'],
                                                                                harvester_queues_errors_cl_ml,
                                                                                harvester_queues[queuename]['badworkers'])
                harvester_computingelements_errors[queuename] = {}
                harvester_computingelements_errors_cl[queuename] = {}
                harvester_computingelements_errors_cl_ml[queuename] = {}

            for ce in harvester_computingelements[queuename]:
                if 'errors' in harvester_computingelements[queuename][ce]:
                    harvester_computingelements_errors[queuename] = errors_object.errors_accounting(ce,
                                                                                                    harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'errors'],
                                                                                                    harvester_computingelements_errors[
                                                                                                            queuename],
                                                                                                    harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'badworkers'])
                    # Cluster logs #
                    harvester_computingelements_errors_cl[queuename] = errors_object_cl.errors_accounting(ce,
                                                                                                          harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'errors'],
                                                                                                          harvester_computingelements_errors_cl[
                                                                                                            queuename],
                                                                                                          harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'badworkers'])
                    # Cluster logs ML#
                    harvester_computingelements_errors_cl_ml[queuename] = errors_object_cl_ml.errors_accounting(ce,
                                                                                                          harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'errors'],
                                                                                                          harvester_computingelements_errors_cl_ml[
                                                                                                            queuename],
                                                                                                          harvester_computingelements[
                                                                                                            queuename][
                                                                                                            ce][
                                                                                                            'badworkers'])
        q_keys = list(harvester_queues.keys())
        q_errors_keys = list(harvester_queues_errors.keys())

        harvester_queues_json_influxdb = []
        harvester_queues_errors_json_influxdb = []

        harvester_queues_errors_json_influxdb_cl = []
        harvester_queues_errors_json_influxdb_cl_ml = []

        harvester_ce_json_influxdb = []
        harvester_ce_errors_json_influxdb = []

        harvester_ce_errors_json_influxdb_cl = []
        harvester_ce_errors_json_influxdb_cl_ml = []

        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1] + '0:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        for queuename, queue in queues.items():
            if queuename in q_keys:
                harvester_queues_json_influxdb.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester'],
                            "resource_type": queue['resource_type']
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": harvester_queues[queuename]['totalworkers'],
                            "goodworkers": harvester_queues[queuename]['goodworkers'],
                            "badworkers": harvester_queues[queuename]['badworkers'],
                        }
                    }
                )
                if queuename in q_errors_keys:
                    for error in harvester_queues_errors[queuename]:
                        harvester_queues_errors_json_influxdb.append(
                            {
                                "measurement": "computingsite_errors",
                                "tags": {
                                    "computingsite": queuename,
                                    "cloud": queue['cloud'],
                                    "atlas_site": queue['atlas_site'],
                                    "errordesc": error,
                                    "harvester": queue['harvester'],
                                    "status": queue['status'],
                                    "resource_type": queue['resource_type']
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_queues[queuename]['totalworkers'],
                                    "goodworkers": harvester_queues[queuename]['goodworkers'],
                                    "badworkers": harvester_queues[queuename]['badworkers'],
                                    "error_count": harvester_queues_errors[queuename][error]['error_count'],
                                    "total_error_count": harvester_queues_errors[queuename][error]['total_error_count'],
                                }
                            }
                        )
                    ### Cluster log PQ###
                    for error in harvester_queues_errors_cl[queuename]:
                        harvester_queues_errors_json_influxdb_cl.append(
                            {
                                "measurement": "computingsite_errors_cl",
                                "tags": {
                                    "computingsite": queuename,
                                    "cloud": queue['cloud'],
                                    "atlas_site": queue['atlas_site'],
                                    "errordesc": error,
                                    "harvester": queue['harvester'],
                                    "status": queue['status'],
                                    "resource_type": queue['resource_type']
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_queues[queuename]['totalworkers'],
                                    "goodworkers": harvester_queues[queuename]['goodworkers'],
                                    "badworkers": harvester_queues[queuename]['badworkers'],
                                    "error_count": harvester_queues_errors_cl[queuename][error]['error_count'],
                                    "total_error_count": harvester_queues_errors_cl[queuename][error]['total_error_count'],
                                }
                            }
                        )
                    ### Cluster log ML PQ###
                    for error in harvester_queues_errors_cl_ml[queuename]:
                        harvester_queues_errors_json_influxdb_cl_ml.append(
                            {
                                "measurement": "computingsite_errors_cl_ml",
                                "tags": {
                                    "computingsite": queuename,
                                    "cloud": queue['cloud'],
                                    "atlas_site": queue['atlas_site'],
                                    "errordesc": error,
                                    "harvester": queue['harvester'],
                                    "status": queue['status'],
                                    "resource_type": queue['resource_type']
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_queues[queuename]['totalworkers'],
                                    "goodworkers": harvester_queues[queuename]['goodworkers'],
                                    "badworkers": harvester_queues[queuename]['badworkers'],
                                    "error_count": harvester_queues_errors_cl_ml[queuename][error]['error_count'],
                                    "total_error_count": harvester_queues_errors_cl_ml[queuename][error]['total_error_count'],
                                }
                            }
                        )
            else:
                harvester_queues_json_influxdb.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester'],
                            "resource_type": queue['resource_type']
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": 0,
                            "goodworkers": 0,
                            "badworkers": 0,
                            "ratio": 0.00
                        }
                    }
                )
        for queuename in harvester_computingelements:
            if queuename not in queues:
                status = 'none'
                cloud = 'none'
                atlas_site = 'none'
                harvester = 'none'
                resource_type = 'none'
            else:
                status = queues[queuename]['status']
                cloud = queues[queuename]['cloud']
                atlas_site = queues[queuename]['atlas_site']
                harvester = queues[queuename]['harvester']
                resource_type = queues[queuename]['resource_type']
            for ce in harvester_computingelements[queuename]:
                harvester_ce_json_influxdb.append(
                    {
                        "measurement": "computingelements",
                        "tags": {
                            "computingsite": queuename,
                            "computingelement": ce,
                            "status": status,
                            "cloud": cloud,
                            "atlas_site": atlas_site,
                            "harvester": harvester,
                            "resource_type": resource_type
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                            "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                            "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                        }
                    }
                )
                if queuename in harvester_computingelements_errors and ce in harvester_computingelements_errors[
                    queuename]:
                    for error in harvester_computingelements_errors[queuename][ce]:
                        harvester_ce_errors_json_influxdb.append(
                            {
                                "measurement": "computingelement_errors",
                                "tags": {
                                    "computingsite": queuename,
                                    "computingelement": ce,
                                    "errordesc": error,
                                    "status": status,
                                    "cloud": cloud,
                                    "atlas_site": atlas_site,
                                    "harvester": harvester,
                                    "resource_type": resource_type
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                                    "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                                    "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                                    "error_count": harvester_computingelements_errors[queuename][ce][error]
                                    ['error_count'],
                                    "total_error_count": harvester_computingelements_errors[queuename][ce][error][
                                        'total_error_count'],
                                }
                            }
                        )
                    # Cluster logs #
                    for error in harvester_computingelements_errors_cl[queuename][ce]:
                        harvester_ce_errors_json_influxdb_cl.append(
                            {
                                "measurement": "computingelement_errors_cl",
                                "tags": {
                                    "computingsite": queuename,
                                    "computingelement": ce,
                                    "errordesc": error,
                                    "status": status,
                                    "cloud": cloud,
                                    "atlas_site": atlas_site,
                                    "harvester": harvester,
                                    "resource_type": resource_type
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                                    "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                                    "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                                    "error_count": harvester_computingelements_errors_cl[queuename][ce][error]
                                    ['error_count'],
                                    "total_error_count": harvester_computingelements_errors_cl[queuename][ce][error][
                                        'total_error_count'],
                                }
                            }
                        )
                    # Cluster logs ML CE#
                    for error in harvester_computingelements_errors_cl_ml[queuename][ce]:
                        harvester_ce_errors_json_influxdb_cl_ml.append(
                            {
                                "measurement": "computingelement_errors_cl_ml",
                                "tags": {
                                    "computingsite": queuename,
                                    "computingelement": ce,
                                    "errordesc": error,
                                    "status": status,
                                    "cloud": cloud,
                                    "atlas_site": atlas_site,
                                    "harvester": harvester,
                                    "resource_type": resource_type
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                                    "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                                    "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                                    "error_count": harvester_computingelements_errors_cl_ml[queuename][ce][error]
                                    ['error_count'],
                                    "total_error_count": harvester_computingelements_errors_cl_ml[queuename][ce][error][
                                        'total_error_count'],
                                }
                            }
                        )
        print('Completed')
        try:
            self.connection.write_points(harvester_queues_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_queues_errors_json_influxdb, time_precision='s',retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_errors_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_queues_errors_json_influxdb_cl, time_precision='s',retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_errors_json_influxdb_cl, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_queues_errors_json_influxdb_cl_ml, time_precision='s',retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(harvester_ce_errors_json_influxdb_cl_ml, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)

    def write_stuck_ces(self):

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        es = PandaQEs(self.path)

        computingelements = {}

        date_key = datetime.now()
        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1] + '0:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())
        #and queue['status'] in ('online')
        for queuename, queue in queues.items():
            if queue['resource_type'] not in ('hpc_special', 'hpc')  \
                    and queue['pilot_manager'] == 'Harvester':
                for ce in queue['queues']:
                    if ce['ce_state'] not in ('DISABLED'):
                        ce_c = ce['ce_endpoint']
                        if '//' in ce_c:
                            ce_c = str(ce['ce_endpoint']).split('//')[-1]
                        if ':' in ce_c:
                            ce_c = str(ce_c).split(':')[0]

                        if ce_c not in computingelements:
                            computingelements[ce_c] = {}
                            if queuename not in computingelements[ce_c]:
                               computingelements[ce_c][queuename] = {}
                        computingelements[ce_c][queuename] = {'ce_state': ce['ce_state']}

        ces_candidats = es.get_stuck_ces(computingelements)

        ces_stuck_json_influxdb = []

        for ce in ces_candidats:
            for cs in ces_candidats[ce]:
                try:
                    status = queues[cs]['status']
                except:
                    status = 'This queue not found in AGIS'

                ces_stuck_json_influxdb.append(
                    {
                        "measurement": "stuck_computingelement",
                        "tags": {
                            "computingsite": cs,
                            "computingelement": ce,
                            "status": status
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "count": 1
                        }
                    }
                )
        try:
            self.connection.write_points(ces_stuck_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)

    def write_stuck_ces_dev(self):

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        es = PandaQEs(self.path)

        computingelements = {}

        date_key = datetime.now()
        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1] + '0:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())
        #and queue['status'] in ('online')
        for queuename, queue in queues.items():
            if queue['resource_type'] not in ('hpc_special', 'hpc')  \
                    and queue['pilot_manager'] == 'Harvester':
                for ce in queue['queues']:
                    if ce['ce_state'] not in ('DISABLED'):
                        ce_c = ce['ce_endpoint']
                        if '//' in ce_c:
                            ce_c = str(ce['ce_endpoint']).split('//')[-1]
                        if ':' in ce_c:
                            ce_c = str(ce_c).split(':')[0]

                        if ce_c not in computingelements:
                            computingelements[ce_c] = {}
                            if queuename not in computingelements[ce_c]:
                               computingelements[ce_c][queuename] = {}
                        computingelements[ce_c][queuename] = {'ce_state': ce['ce_state']}

        ces_candidats = es.get_stuck_ces_v1()

        ces_stuck_json_influxdb = []
        for ce in ces_candidats:
            for cs in ces_candidats[ce]:
                try:
                    status = queues[cs]['status']
                except:
                    status = 'This queue not found in AGIS'

                ces_stuck_json_influxdb.append(
                    {
                        "measurement": "stuck_computingelement",
                        "tags": {
                            "computingsite": cs,
                            "computingelement": ce,
                            "status": status
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "count": 1
                        }
                    }
                )

        try:
            self.connection.write_points(ces_stuck_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)

    def write_suspicious_elements(self):

        es = PandaQEs(self.path)

        date_key = datetime.now()
        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-2] + '00:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

        time_stamp = time.mktime(datetime_object.timetuple())

        inactive_candidats = es.get_inactive_elements()
        pq_candidats_avg, pq_candidats_count = es.get_suspicious_pq()

        pq_candidats_count_json_influxdb = []
        inactivate_candidats_count_json_influxdb = []

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()
        q_keys = pq_candidats_count.keys()

        for queuename, queue in queues.items():
            if queuename in q_keys:
                pq_candidats_count_json_influxdb.append(
                    {
                        "measurement": "suspicious_computingsite",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester'],
                            "resource_type": queue['resource_type']
                        },
                        "time": int(time_stamp),
                        "fields": pq_candidats_count[queuename]
                    }
                )


        q_keys = inactive_candidats.keys()

        for queuename, queue in queues.items():
            if queuename in q_keys:
                for computingelment in inactive_candidats[queuename].keys():
                    inactivate_candidats_count_json_influxdb.append(
                        {
                            "measurement": "inactive_elements",
                            "tags": {
                                "computingsite": queuename,
                                "computingelement": computingelment,
                                "cloud": queue['cloud'],
                                "atlas_site": queue['atlas_site'],
                                "status": queue['status'],
                                "harvester": queue['harvester'],
                                "resource_type": queue['resource_type'],
                                "corecount": str(queue['corecount'])

                            },
                            "time": int(time_stamp),
                            "fields": inactive_candidats[queuename][computingelment]
                        }
                    )
        print('Completed')
        try:
            self.connection.write_points(pq_candidats_count_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(inactivate_candidats_count_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)

    def write_workers_jobs(self):

        pq_running_stats = PandaDBPQ(self.path).get_running_workers_jobs()
        pq_completed_stats = PandaDBPQ(self.path).get_running_workers_completed_jobs()

        date_key = datetime.now()
        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-2] + '00:00'
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

        time_stamp = time.mktime(datetime_object.timetuple())

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'

        resp = requests.get(url)
        queues = resp.json()
        q_run_keys = pq_running_stats.keys()
        q_completed_keys = pq_completed_stats.keys()

        running_workers_jobs_json_influxdb = []
        completed_workers_jobs_json_influxdb = []

        for queuename, queue in queues.items():
            if queuename in q_run_keys:
                running_workers_jobs_json_influxdb.append(
                    {
                        "measurement": "running_workers_jobs",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester'],
                            "resource_type": queue['resource_type']
                        },
                        "time": int(time_stamp),
                        "fields": pq_running_stats[queuename]
                    }
                )

        for queuename, queue in queues.items():
            if queuename in q_completed_keys:
                completed_workers_jobs_json_influxdb.append(
                    {
                        "measurement": "completed_workers_jobs",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue['cloud'],
                            "atlas_site": queue['atlas_site'],
                            "status": queue['status'],
                            "harvester": queue['harvester'],
                            "resource_type": queue['resource_type']
                        },
                        "time": int(time_stamp),
                        "fields": pq_completed_stats[queuename]
                    }
                )
        print("Completed")
        try:
            self.connection.write_points(running_workers_jobs_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
            self.connection.write_points(completed_workers_jobs_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)