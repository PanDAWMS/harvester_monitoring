from os import sys

from datetime import datetime

import getopt
from configparser import ConfigParser
import subprocess, socket, re, cx_Oracle, requests, json, shutil

from logger import ServiceLogger

_logger = ServiceLogger("cron",  __file__).logger


def make_db_connection(cfg):
    try:
        dbuser = cfg.get('pandadb', 'login')
        dbpasswd = cfg.get('pandadb', 'password')
        description = cfg.get('pandadb', 'description')
    except:
        _logger.error('Settings for Oracle connection not found')
        return None
    try:
        connection = cx_Oracle.connect(dbuser, dbpasswd, description)
        _logger.debug('DB connection established. "{0}" "{1}"'.format(dbuser, description))
        return connection
    except Exception as ex:
        _logger.error(ex)
        return None


def logstash_configs(cfg):
    try:
        url = cfg.get('logstash', 'url')
        port = cfg.get('logstash', 'port')
        auth = [x.strip() for x in cfg.get('logstash', 'auth').split(',')]
        auth = (auth[0],auth[1])
        _logger.debug('Logstash settings have been read. "{0}" "{1}" "{2}"'.format(url, port, auth))
        return url, port, auth
    except:
        _logger.error('Settings for logstash not found')
        return None, None, None


def servers_configs(cfg):
    try:
        disk_list = [x.strip() for x in cfg.get('othersettings', 'disks').split(',')]
        process_list = [x.strip() for x in cfg.get('othersettings', 'processes').split(',')]
        _logger.debug('Server settings have been read. Disk list: {0}. Process list: {1}'.format(disk_list, process_list))
        return disk_list, process_list,
    except:
        _logger.error('Settings for servers configs not found')
        return None, None


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

def volume_use(path: str):
    try:
        total, used, free = shutil.disk_usage('/' + path.lstrip('/'))
        if total == 0:
            return None
        return used / total * 100.0
    except Exception:
        return None

def volume_use_old(volume_name):
    command = "df -Pkh /" + volume_name
    used_amount = 0
    tmp_array = command.split()
    try:
        output = subprocess.Popen(tmp_array, stdout=subprocess.PIPE).communicate()[0].decode("utf-8")
    except:
        return None
    for line in output.split('\n'):
        if re.search(volume_name, line):
            used_amount = re.search(r"(\d+)\%", line).group(1)
    if used_amount == 0:
        _logger.debug('df: "{0}": No such file or directory'.format(volume_name))
    try:
        used_amount_float = float(used_amount)
    except ValueError:
        used_amount_float = None

    return used_amount_float


def process_availability(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)
    output = subprocess.Popen("ps -eo pgid,args | grep {0} | grep -v grep | uniq".format(process_name),
                              stdout=subprocess.PIPE, shell=True).communicate()[0].decode("utf-8")
    count = 0
    for line in output.split('\n'):
        line = line.strip()
        if line == '':
            continue
        count += 1
    if count > 0:
        availability = '100'
        avail_info = '{0} running'.format(process_name)

    return availability, avail_info


def get_workers(submissionhost, settings):
    dict = {}

    query = """
    SELECT status, count(*) FROM (SELECT
    CASE 
        WHEN status IN ('submitted', 'running', 'finished') THEN 'n_workers_healthy_states'
        WHEN status IN ('missed', 'cancelled', 'failed') THEN 'n_workers_bad_states'
        ELSE 'n_workers_other_states'
    END AS status
    FROM
        atlas_panda.harvester_workers WHERE SUBMITTIME > CAST (sys_extract_utc(SYSTIMESTAMP) - interval '30' minute as DATE) AND SUBMISSIONHOST like '{0}%') GROUP BY status
    """.format(submissionhost)

    connection = make_db_connection(settings)

    with connection:
        sum = 0
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            for res in results:
                sum += res[1]
                dict[res[0]] = res[1]
            dict['n_workers_created_total'] = sum
            return dict
        finally:
            if cursor is not None:
                cursor.close()


def send_data(data, settings):
   url, port, auth = logstash_configs(settings)
   try:
        code = requests.post(
        url='http://{0}:{1}'.format(url, port),
        data=data,
        auth=auth)
        if code.status_code == 200:
            _logger.debug('Status code: {0}'.format(code.status_code))
        else:
            _logger.error('Status code: {0}'.format(code.status_code))
   except Exception as ex:
        _logger.debug('Data can not be sent. {0}'.format(ex))


def get_settings_path(argv):
   cfg = ConfigParser()

   path = ''
   try:
      opts, args = getopt.getopt(argv,"hi:s:",["settings="])
   except getopt.GetoptError:
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-s' or opt == '-settings':
          path = str(arg)
   if path == '':
       path = 'cron_settings.ini'
   cfg.read(path)
   if cfg.has_section('logstash') and cfg.has_section('logstash') and cfg.has_section('logstash'):
       return cfg
   else:
       _logger.error('Settings file not found. {0}'.format(path))
   return None


def main():
    settings_path = get_settings_path(sys.argv[1:])
    if settings_path is not None:
        hostname = socket.gethostname()
        disk_list, process_list = servers_configs(settings_path)

        dict = get_workers(hostname, settings_path)

        dict['submissionhost'] = hostname

        for disk in disk_list:
            dict['disk_usage_'+disk] = volume_use(disk)

        for process in process_list:
            proc_avail, proc_avail_info = process_availability(process)
            dict[process] = proc_avail
            dict[process+'_info'] = proc_avail_info

        dict['creation_time'] = datetime.utcnow()
        send_data(json.dumps(dict, cls=DateTimeEncoder), settings_path)

if __name__=='__main__':
    main()
