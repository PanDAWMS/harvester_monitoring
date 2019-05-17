from datetime import datetime
from configparser import ConfigParser
import subprocess, socket, re, cx_Oracle, requests, json

def make_db_connection():
    try:
        cfg = ConfigParser()
        cfg.read('cron_settings.ini')
        dbuser = cfg.get('pandadb', 'login')
        dbpasswd = cfg.get('pandadb', 'password')
        description = cfg.get('pandadb', 'description')
    except:
        return None
    try:
        connection = cx_Oracle.connect(dbuser, dbpasswd, description)
        return connection
    except:
        return None

def logstash_configs():
    try:
        cfg = ConfigParser()
        cfg.read('cron_settings.ini')
        url = cfg.get('logstash', 'url')
        port = cfg.get('logstash', 'port')
        auth = cfg.get('logstash', 'auth')
        return url, port, auth
    except:
        return None, None, None

def servers_configs():
    try:
        cfg = ConfigParser()
        cfg.read('cron_settings.ini')
        disk_list = list(cfg.get('othersettings', 'disks').split(','))
        process_list = list(cfg.get('othersettings', 'processes').split(','))
        return disk_list, process_list,
    except:
        return None, None

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

def volume_use(volume_name):
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
    try:
        used_amount_float = float(used_amount)
    except ValueError:
        used_amount_float = None

    return used_amount_float

def process_availability(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)
    output = subprocess.Popen("ps -eo pgid,args | grep {0} | grep -v grep | uniq".format(process_name),
                              stdout=subprocess.PIPE, shell=True).communicate()[0]
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

def get_workers(submissionhost):
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

    connection = make_db_connection()

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

def send_data(data):
   url, port, auth = logstash_configs()
   requests.post(
        url='http://{0}:{1}'.format(url, port),
        data=data,
        auth=auth)

def main():
    hostname = socket.gethostname()
    disk_list, process_list = servers_configs()

    dict = get_workers(hostname)

    dict['submitssionhost'] = hostname

    for disk in disk_list:
        dict['disk_usage_'+disk] = volume_use(disk)

    for process in process_list:
        proc_avail, proc_avail_info = process_availability(process)
        dict[process] = proc_avail
        dict[process+'_info'] = proc_avail_info

    dict['creation_time'] = datetime.utcnow()
    send_data(json.dumps(dict, cls=DateTimeEncoder))

main()