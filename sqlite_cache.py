import sqlite3
from datetime import datetime, timedelta
from config import Config

class Sqlite:
    def __init__(self, path, configs = None):
        self.connection = self.__make_connection(path)
        self.instancesconfigs = configs

    "private method"
    def __make_connection(self, path):
        """
        Create a connection to the SQLite cache
        """
        try:
            connection = sqlite3.connect(path)
            return connection
        except sqlite3.Error as e:
            pass
        return None

    def get_instances(self):
        """
        Getting instances from SQLite cache
        """
        connection = self.connection

        instances = []

        connection.row_factory = sqlite3.Row
        cur = connection.cursor()
        cur.execute("SELECT * FROM INSTANCES")
        rows = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            object = dict(zip(columns, row))
            instances.append(object)

        instancesNoneDict = {}

        for instance in instances:
            if instance['harvesterid'] not in instancesNoneDict:
                instancesNoneDict[instance['harvesterid']] = {}
                if instance['harvesterhost'] not in instancesNoneDict[instance['harvesterid']]:
                    instancesNoneDict[instance['harvesterid']][instance['harvesterhost']] = {
                        'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'],
                        'contacts': instance['contacts'].split(','),
                        'active': instance['active'], 'notificated': instance['notificated']}
            elif instance['harvesterid'] in instancesNoneDict:
                if instance['harvesterhost'] not in instancesNoneDict[instance['harvesterid']]:
                    instancesNoneDict[instance['harvesterid']][instance['harvesterhost']] = {
                        'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'],
                        'contacts': instance['contacts'].split(','),
                        'active': instance['active'], 'notificated': instance['notificated']}
                    if 'none' in instancesNoneDict[instance['harvesterid']]:
                        del instancesNoneDict[instance['harvesterid']]['none']
        return instancesNoneDict

    def update_field(self, field, value, harvesterid, harvesterhost):
        """
        Query update field in SQLite cache
        :param conn: the Connection object
        :param field: column name in SQLite database
        :param value: value of the column name in SQLite database
        :param harvesterid: Harvester instance
        :param harvesterhost: Harvester host
        """
        connection = self.connection
        query = """UPDATE INSTANCES SET {0} = ?  WHERE harvesterid = ? and harvesterhost = ?""".format(field)
        cur = connection.cursor()

        cur.execute(query, (value,
                            harvesterid, harvesterhost))
        connection.commit()

    def instances_availability(self, lastsubmitedinstance, metrics):
        """
        Check instances for availability and update SQLite cache
        """
        connection = self.connection
        instancesconfig = self.instancesconfigs

        cur = connection.cursor()
        harvesters = instancesconfig.keys()
        connection.row_factory = sqlite3.Row

        for harvesterid in harvesters:
            avaibility = 100
            errorsdesc = ''
            instanceisenable = self.__str_to_bool(instancesconfig[harvesterid]['instanceisenable'])
            del instancesconfig[harvesterid]['instanceisenable']
            ### Instance is enable ###
            if instanceisenable:
                ### No submitted worker ###
                for host in lastsubmitedinstance[harvesterid]['harvesterhost']:
                    mins = 30
                    if host != 'none' and host in instancesconfig[harvesterid]:
                        mins = int(instancesconfig[harvesterid][host]['lastsubmittedworker'])
                        if lastsubmitedinstance[harvesterid]['harvesterhost'][host][
                            'harvesterhostmaxtime'] < datetime.utcnow() - timedelta(
                                minutes=mins):
                            errorsdesc = errorsdesc + "Last submitted worker was {0}".format(
                                str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime'])) + '\n'
                            avaibility = 0
                ### No heartbeat ###
                for host in instancesconfig[harvesterid].keys():
                    if host in metrics[harvesterid] and bool(instancesconfig[harvesterid][host]['hostisenable']):
                        heartbeattime = metrics[harvesterid][host].keys()[0]
                        contacts = instancesconfig[harvesterid][host]['contacts']
                        if heartbeattime < datetime.utcnow() - timedelta(
                                minutes=int(instancesconfig[harvesterid][host]['lastheartbeat'])):
                            errorsdesc = errorsdesc + "Last heartbeat was {0}".format(
                                str(heartbeattime)) + '\n'
                            avaibility = 0
                        #### Metrics ####
                        if avaibility == 100:
                            memory = instancesconfig[harvesterid][host]['memory']
                            cpu_warning = instancesconfig[harvesterid][host]['metrics']['cpu_warning']
                            cpu_critical = instancesconfig[harvesterid][host]['metrics']['cpu_critical']
                            disk_warning = instancesconfig[harvesterid][host]['metrics']['disk_warning']
                            disk_critical = instancesconfig[harvesterid][host]['metrics']['disk_critical']
                            #### Metrics DB ####
                            for metric in metrics[harvesterid][host][heartbeattime]:
                                cpu_pc = int(metric['cpu_pc'])
                                free_mib = int(self.__get_change(metric['rss_mib'], memory))
                                if 'volume_data_pc' in metric:
                                    volume_data_pc = int(metric['volume_data_pc'])
                                else:
                                    volume_data_pc = -1

                                #### Memory ####
                                if free_mib <= 50:
                                    avaibility = 50
                                    errorsdesc = errorsdesc + "Warning!. Memory consumption: {0}".format(
                                        str(free_mib)) + '\n'
                                elif free_mib <= 10:
                                    errorsdesc = errorsdesc + "Memory consumption: {0}".format(
                                        str(free_mib)) + '\n'
                                    avaibility = 0
                                #### CPU ####
                                if cpu_pc >= cpu_warning:
                                    avaibility = 50
                                    errorsdesc = errorsdesc + "Warning!. CPU utilization: {0}".format(
                                        str(cpu_pc)) + '\n'
                                elif cpu_pc >= cpu_critical:
                                    errorsdesc = errorsdesc + "CPU utilization: {0}".format(
                                        str(cpu_pc)) + '\n'
                                    avaibility = 10
                                #### HDD ####
                                if volume_data_pc >= disk_warning:
                                    avaibility = 50
                                    errorsdesc = errorsdesc + "Warning!. Disk utilization: {0}".format(
                                        str(volume_data_pc)) + '\n'
                                elif volume_data_pc >= disk_critical:
                                    errorsdesc = errorsdesc + "Disk utilization: {0}".format(
                                        str(volume_data_pc)) + '\n'
                                    avaibility = 10
                                #### HDD1 ####
                                if 'volume_data1_pc' in metric:
                                    volume_data1_pc = int(metric['volume_data1_pc'])
                                    if volume_data1_pc >= disk_warning:
                                        avaibility = 50
                                        errorsdesc = errorsdesc + "Warning!. Disk 1 utilization: {0}".format(
                                            str(volume_data1_pc)) + '\n'
                                    elif volume_data1_pc >= disk_critical:
                                        errorsdesc = errorsdesc + "Disk 1 utilization: {0}".format(
                                            str(volume_data1_pc)) + '\n'
                                        avaibility = 10
                    try:
                        cur.execute("insert into INSTANCES values (?,?,?,?,?,?,?,?,?)",
                                    (str(harvesterid), str(host),
                                     str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                     heartbeattime, 1, 0, avaibility, str(contacts), str(errorsdesc)))
                        connection.commit()
                    except:
                        query = \
                            """UPDATE INSTANCES 
        SET lastsubmitted = '{0}', active = {1}, availability = {2}, lastheartbeat = '{3}', contacts = '{4}', errorsdesc = '{5}'
        WHERE harvesterid = '{6}' and harvesterhost = '{7}'
                            """.format(str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                       1, avaibility, heartbeattime, str(contacts), str(errorsdesc), str(harvesterid),
                                       str(host))

                        cur.execute(query)
                        connection.commit()
            else:
                cur.execute("DELETE FROM INSTANCES WHERE harvesterid = ?", [str(harvesterid)])
                connection.commit()

    "private method"
    def __get_change(self, current, previous):
        """
        Get difference in percent
        """
        current = float(current)
        previous = float(previous)
        if current == previous:
            return 0
        try:
            r = (abs(current - previous) / previous) * 100
            if r > 100:
                r = 100
            return round(r)
        except ZeroDivisionError:
            return 100

    "private method"
    def __str_to_bool(self, s):
        """
        Convert XML string fields to bool type
        """
        if s == 'True':
            return True
        elif s == 'False':
            return False
        else:
            raise ValueError