import requests
import time
import copy
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

    def write_data_tmp(self, tdelta):

        es = Es(self.path)

        tmp_harvester_schedd = es.get_info_workers(tdelta=tdelta, type="gahp", time='submittime')
        harvester_schedd = copy.deepcopy(tmp_harvester_schedd)
        errors_object = Errors('patterns.txt')

        harvester_schedd_errors = {}

        for schedd in tmp_harvester_schedd:
            harvester_schedd_errors[schedd]={}
            for ce in tmp_harvester_schedd[schedd]:
                if 'errors' in tmp_harvester_schedd[schedd][ce]:
                    harvester_schedd_errors[schedd] = errors_object.errors_accounting_tmp(ce, harvester_schedd[schedd][ce]['errors'],
                                                                                 harvester_schedd_errors[schedd], harvester_schedd[schedd][ce]['badworkers'])
        harvester_schedd_json_influxdb = []
        harvester_schedd_errors_json_influxdb = []

        for ce in tmp_harvester_schedd:
            if ce == 'none':
                del harvester_schedd[ce]
                continue
            error_list = []
            if len(tmp_harvester_schedd[ce].keys()) > 1:
                for schedd in tmp_harvester_schedd[ce]:
                    if schedd == 'none' or schedd == 'LOCAL':
                        del harvester_schedd[ce][schedd]
                        continue
                    try:
                        error_rate = (float(tmp_harvester_schedd[ce][schedd]['badworkers'])/(float(tmp_harvester_schedd[ce][schedd]['goodworkers']) + float(tmp_harvester_schedd[ce][schedd]['badworkers'])))*100
                        error_list.append(int(error_rate))
                    except:
                        pass
                min_value = min(error_list)
                max_value = max(error_list)
                if max_value == 100 and (min_value == 0 or min_value < 10):
                    pass
                else:
                    del harvester_schedd[ce]
            else:
                try:
                    error_rate = (float(tmp_harvester_schedd[ce][schedd]['badworkers']) / (float(tmp_harvester_schedd[ce][schedd]['goodworkers']) + float(tmp_harvester_schedd[ce][schedd]['badworkers']))) * 100
                except:
                    error_rate = 0
                if error_rate == 100:
                    pass
                else:
                    del harvester_schedd[ce]

        date_key = datetime.now()
        date_string = date_key.strftime("%Y-%m-%d %H:%M")[:-1]+'0:00'
        datetime_object = datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S')

        time_stamp = time.mktime(datetime_object.timetuple())

        for ce in harvester_schedd:
            for schedd in harvester_schedd[ce]:
                harvester_schedd_json_influxdb.append(
                    {
                        "measurement": "submissionhosts",
                        "tags": {
                            "submissionhost": schedd,
                            "computingelement": ce,
                            "computingsite": harvester_schedd[ce][schedd]['computingsite']
                        },
                        "time": int(time_stamp),
                        "fields": {
                            "totalworkers": harvester_schedd[ce][schedd]['totalworkers'],
                            "goodworkers": harvester_schedd[ce][schedd]['goodworkers'],
                            "badworkers": harvester_schedd[ce][schedd]['badworkers'],
                        }
                    }
                )
                if schedd in harvester_schedd_errors and ce in harvester_schedd_errors[schedd]:
                    for error in harvester_schedd_errors[schedd][ce]:
                        harvester_schedd_errors_json_influxdb.append(
                            {
                                "measurement": "submissionhost_errors",
                                "tags": {
                                    "submissionhost": schedd,
                                    "computingelement": ce,
                                    "computingsite": harvester_schedd[ce][schedd]['computingsite'],
                                    "errordesc": error,
                                },
                                "time": int(time_stamp),
                                "fields": {
                                    "totalworkers": harvester_schedd[ce][schedd]['totalworkers'],
                                    "goodworkers": harvester_schedd[ce][schedd]['goodworkers'],
                                    "badworkers": harvester_schedd[ce][schedd]['badworkers'],
                                    # "ratio": float(harvester_computingelements[ce]['ratio']),
                                    "error_count": harvester_schedd_errors[ce][schedd][error]['error_count'],
                                    "total_error_count": harvester_schedd_errors[ce][schedd][error][
                                        'total_error_count'],
                                }
                            }
                        )

        try:
           self.connection.write_points(harvester_schedd_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)
        try:
           self.connection.write_points(harvester_schedd_errors_json_influxdb, time_precision='s', retention_policy="main")
        except Exception as ex:
            _logger.error(ex)