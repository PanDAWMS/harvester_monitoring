import requests
import time

from datetime import datetime
from configparser import ConfigParser
from influxdb import InfluxDBClient

from es import Es

from logger import ServiceLogger
from accounting.error_accounting import Errors

_logger = ServiceLogger("influxdb").logger

class Influx:

    def __init__(self, path):
        self.connection = self.__make_connection(path=path)
        self.path = path

    # private method
    def __make_connection(self, path):
        """
        Create a connection to InfluxDB
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            user = cfg.get('influxdb', 'login')
            password = cfg.get('influxdb', 'password')
            dbname = cfg.get('influxdb', 'dbname')
            host = cfg.get('influxdb', 'host')
            port = cfg.get('influxdb', 'port')

        except Exception as ex:
            _logger.error(ex.message)
            print ex.message
        try:
            connection = InfluxDBClient(host, int(port), user, password, dbname, ssl=True,
                                        verify_ssl=False)
            return connection
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

        q_keys = harvester_queues.keys()
        q_errors_keys = harvester_queues_errors.keys()

        harvester_queues_json_influxdb = []
        harvester_queues_errors_json_influxdb = []

        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1]+'0:00'
        datetime_object = datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        for queuename, queue in queues.iteritems():
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
                                    #"ratio": float(harvester_queues[queuename]['ratio']),
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

        ce_errors_keys = harvester_computingelements_errors.keys()

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
                                #"ratio": float(harvester_computingelements[ce]['ratio']),
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

    def write_data_tmp(self, tdelta):

        date_key = datetime.now()

        es = Es(self.path)

        harvester_queues, harvester_computingelements = es.get_info_workers(tdelta=tdelta, type="ce_pq", time='endtime')

        errors_object = Errors('patterns.txt')

        harvester_queues_errors = {}
        harvester_computingelements_errors = {}

        for queuename in harvester_queues:
            if 'errors' in harvester_queues[queuename]:
                harvester_queues_errors = errors_object.errors_accounting_tmp(queuename, harvester_queues[queuename]['errors'],
                                                                              harvester_queues_errors, harvester_queues[queuename]['badworkers'])
                harvester_computingelements_errors[queuename] = {}
            for ce in harvester_computingelements[queuename]:
                if 'errors' in harvester_computingelements[queuename][ce]:
                    harvester_computingelements_errors[queuename] = errors_object.errors_accounting_tmp(ce, harvester_computingelements[queuename][ce]['errors'],
                                                                                                        harvester_computingelements_errors[queuename], harvester_computingelements[queuename][ce]['badworkers'])

        q_keys = harvester_queues.keys()
        q_errors_keys = harvester_queues_errors.keys()

        harvester_queues_json_influxdb = []
        harvester_queues_errors_json_influxdb = []

        harvester_ce_json_influxdb = []
        harvester_ce_errors_json_influxdb = []

        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1]+'0:00'
        datetime_object = datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(datetime_object.timetuple())

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()

        for queuename, queue in queues.iteritems():
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
                                    #"ratio": float(harvester_queues[queuename]['ratio']),
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
        for queuename in harvester_computingelements:
            if queuename not in queues:
                status = 'none'
                cloud = 'none'
                atlas_site = 'none'
                harvester = 'none'
            else:
                status = queues[queuename]['status']
                cloud = queues[queuename]['cloud']
                atlas_site = queues[queuename]['atlas_site']
                harvester = queues[queuename]['harvester']

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
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                            "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                            "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                        }
                    }
                )
                if queuename in harvester_computingelements_errors and ce in harvester_computingelements_errors[queuename]:
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
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_computingelements[queuename][ce]['totalworkers'],
                                    "goodworkers": harvester_computingelements[queuename][ce]['goodworkers'],
                                    "badworkers": harvester_computingelements[queuename][ce]['badworkers'],
                                    # "ratio": float(harvester_computingelements[ce]['ratio']),
                                    "error_count": harvester_computingelements_errors[queuename][ce][error]['error_count'],
                                    "total_error_count": harvester_computingelements_errors[queuename][ce][error][
                                        'total_error_count'],
                                }
                            }
                        )
        #
        # for schedd in harvester_schedd:
        #     for ce in harvester_schedd[schedd]:
        #         harvester_schedd_json_influxdb.append(
        #             {
        #                 "measurement": "submissionhosts",
        #                 "tags": {
        #                     "submissionhost": schedd,
        #                     "computingelement": ce
        #                 },
        #                 "time": int(time_stamp),
        #                 "fields": {
        #                     "totalworkers": harvester_schedd[schedd][ce]['totalworkers'],
        #                     "goodworkers": harvester_schedd[schedd][ce]['goodworkers'],
        #                     "badworkers": harvester_schedd[schedd][ce]['badworkers'],
        #                 }
        #             }
        #         )
        #         if schedd in harvester_schedd_errors and ce in harvester_schedd_errors[schedd]:
        #             for error in harvester_schedd_errors[schedd][ce]:
        #                 harvester_schedd_errors_json_influxdb.append(
        #                     {
        #                         "measurement": "submissionhost_errors",
        #                         "tags": {
        #                             "submissionhost": schedd,
        #                             "computingelement": ce,
        #                             "errordesc": error,
        #                         },
        #                         "time": int(time_stamp),
        #                         "fields": {
        #                             "totalworkers": harvester_schedd[schedd][ce]['totalworkers'],
        #                             "goodworkers": harvester_schedd[schedd][ce]['goodworkers'],
        #                             "badworkers": harvester_schedd[schedd][ce]['badworkers'],
        #                             # "ratio": float(harvester_computingelements[ce]['ratio']),
        #                             "error_count": harvester_schedd_errors[schedd][ce][error]['error_count'],
        #                             "total_error_count": harvester_schedd_errors[schedd][ce][error][
        #                                 'total_error_count'],
        #                         }
        #                     }
        #                 )

        try:
           self.connection.write_points(harvester_queues_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
           self.connection.write_points(harvester_queues_errors_json_influxdb, time_precision='s', retention_policy="main")
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
