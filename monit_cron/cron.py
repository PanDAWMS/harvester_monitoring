import getopt, subprocess, socket, re, cx_Oracle, requests, json, psutil, numpy as np

from os import sys
from datetime import datetime
from configparser import ConfigParser
from logger import ServiceLogger

_logger = ServiceLogger("cron", __file__).logger

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

def cpu_info():
    cpu_times = psutil.cpu_times()
    cpu_usage_list = []
    for x in range(5):
        cpu_usage_list.append(psutil.cpu_percent(interval=2, percpu=True))
    return cpu_times, cpu_usage_list

def memory_info():
    memory_virtual = psutil.virtual_memory()
    memory_swap = psutil.swap_memory()
    return memory_virtual, memory_swap

def disk_info(disk=''):
    if disk == '':
        full_path = '/'
    else:
        full_path = '/' + disk
    disk_usage = psutil.disk_usage(full_path)
    return disk_usage

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
        auth = (auth[0], auth[1])
        _logger.debug('Logstash settings have been read. "{0}" "{1}" "{2}"'.format(url, port, auth))
        return url, port, auth
    except:
        _logger.error('Settings for logstash not found')
        return None, None, None

def servers_configs(cfg):
    try:
        metrics = [x.strip() for x in cfg.get('othersettings', 'metrics').split(',')]
        disk_list = [x.strip() for x in cfg.get('othersettings', 'disks').split(',')]
        process_list = [x.strip() for x in cfg.get('othersettings', 'processes').split(',')]
        _logger.debug(
            'Server settings have been read. Disk list: {0}. Process list: {1}'.format(disk_list, process_list))
        return metrics, disk_list, process_list
    except:
        _logger.error('Settings for servers configs not found')
        return None, None

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
    if used_amount == 0:
        _logger.debug('df: "{0}": No such file or directory'.format(volume_name))
    try:
        used_amount_float = float(used_amount)
    except ValueError:
        used_amount_float = None

    return used_amount_float

def process_availability_psutil(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)
    for proc in psutil.process_iter():
        process = psutil.Process(proc.pid)  # Get the process info using PID
        pname = process.name()  # Here is the process name
        if pname == process_name:
            availability = '100'
            avail_info = '{0} running'.format(process_name)

    return availability, avail_info

def process_availability(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)

    output = subprocess.Popen("ps -eo pgid,args | grep {0} | grep -v grep | uniq".format(process_name),
                              stdout=subprocess.PIPE, shell=True).communicate()[0]
    if str(output) != "b''":
        availability = '100'
        avail_info = '{0} running'.format(process_name)
    return availability, avail_info

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
    path, type = '', ''
    try:
        opts, args = getopt.getopt(argv, "hi:s:t:", ["settings=", "type="])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-s' or opt == '-settings':
            path = str(arg)
        if opt == '-t' or opt == '-type':
            type = str(arg)
    if path == '':
        path = 'cron_settings.ini'
    if type == '':
        type = None
    cfg.read(path)
    if cfg.has_section('logstash') and cfg.has_section('logstash') and cfg.has_section('logstash'):
        return cfg, type
    else:
        _logger.error('Settings file not found. {0}'.format(path))
    return None, None

def main():
    dict_metrics = {}

    settings_path, type = get_settings_path(sys.argv[1:])

    if settings_path is not None:
        hostname = socket.gethostname()
        dict_metrics['hostname'] = hostname
        metrics, disk_list, process_list = servers_configs(settings_path)

        if 'cpu' in metrics:
            cpu_times, cpu_usage = cpu_info()
            dict_metrics['avg_cpu_usage'] = np.array(cpu_usage).mean() / 100

        if 'memory' in metrics:
            memory_virtual, memory_swap = memory_info()
            dict_metrics['memory_active'] = memory_virtual.active
            dict_metrics['memory_available'] = memory_virtual.available
            dict_metrics['memory_buffers'] = memory_virtual.buffers
            dict_metrics['memory_cached'] = memory_virtual.cached
            dict_metrics['memory_free'] = memory_virtual.free
            dict_metrics['memory_shared'] = memory_virtual.shared
            dict_metrics['memory_slab'] = memory_virtual.slab
            dict_metrics['memory_total'] = memory_virtual.total
            dict_metrics['memory_used'] = memory_virtual.used
            dict_metrics['memory_usage_pc'] = memory_virtual.percent
            dict_metrics['memory_free_pc'] = memory_virtual.available * 100 / memory_virtual.total

            dict_metrics['swap_free'] = memory_swap.free
            dict_metrics['swap_sin'] = memory_swap.sin
            dict_metrics['swap_sout'] = memory_swap.sout
            dict_metrics['swap_total'] = memory_swap.total
            dict_metrics['swap_used'] = memory_swap.used
            dict_metrics['swap_usage_pc'] = memory_swap.percent
            try:
                dict_metrics['swap_free_pc'] = (memory_swap.free / memory_swap.total) * 100
            except:
                dict_metrics['swap_free_pc'] = 0

        if 'disk' in metrics:
            for diskname in disk_list:
                disk = disk_info(diskname)
                dict_metrics[diskname + '_total'] = disk.total
                dict_metrics[diskname + '_used'] = disk.used
                dict_metrics[diskname + '_free'] = disk.free
                dict_metrics[diskname + '_usage_pc'] = disk.percent
                dict_metrics[diskname + '_free_pc'] = (disk.free / disk.total) * 100

        if 'process' in metrics:
            for process in process_list:
                proc_avail, proc_avail_info = process_availability(process)
                dict_metrics[process] = proc_avail
                dict_metrics[process + '_info'] = proc_avail_info

        dict_metrics['creation_time'] = datetime.utcnow()
        if type is not None:
            dict_metrics['monittype'] = type
            send_data(json.dumps(dict_metrics, cls=DateTimeEncoder), settings_path)
            _logger.info("data has been sent {0}".format(json.dumps(dict_metrics,cls=DateTimeEncoder)))
        else:
            _logger.error("Type is not defined")


if __name__ == '__main__':
    main()
