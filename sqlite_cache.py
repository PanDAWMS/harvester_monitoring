import sqlite3
from datetime import datetime, timedelta
from logger import ServiceLogger

_logger = ServiceLogger("sqlitecache").logger

class Sqlite:
    def __init__(self, path, configs = None):
        self.connection = self.__make_connection(path)
        self.instancesconfigs = configs

   # private method
    def __make_connection(self, path):
        """
        Create a connection to the SQLite cache
        """
        try:
            connection = sqlite3.connect(path)
            return connection
        except sqlite3.Error as ex:
            _logger.error(ex.message)
            print ex.message
        return None

    def get_instances(self):
        """
        Getting instances from SQLite cache
        """
        connection = self.connection

        instances = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute("SELECT * FROM INSTANCES")
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
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
        try:
            connection = self.connection
            query = """UPDATE INSTANCES SET {0} = ?  WHERE harvesterid = ? and harvesterhost = ?""".format(field)
            cur = connection.cursor()

            cur.execute(query, (value,
                                harvesterid, harvesterhost))
            connection.commit()
            _logger.info("The {0} field was updated by the query '{1}'".format(field, query))
        except Exception as ex:
            _logger.error(ex.message)

    def instances_availability(self, lastsubmitedinstance, metrics):
        """
        Check instances for availability and update SQLite cache
        """
        try:
            if lastsubmitedinstance is None:
                raise ValueError('lastsubmitedinstance is None')
            elif metrics is None:
                raise ValueError('metrics is None')
            connection = self.connection
            instancesconfig = self.instancesconfigs

            cur = connection.cursor()
            harvesters = instancesconfig.keys()
            connection.row_factory = sqlite3.Row

            for harvesterid in harvesters:
                error_text = set()

                instanceisenable = self.__str_to_bool(instancesconfig[harvesterid]['instanceisenable'])
                del instancesconfig[harvesterid]['instanceisenable']
                ### Instance is enable ###
                if instanceisenable:
                    for host in instancesconfig[harvesterid].keys():
                        avaibility = []
                        if self.__str_to_bool(instancesconfig[harvesterid][host]['hostisenable']):
                            ### No submitted worker ###
                            timedelta_submitted = timedelta(minutes=30)
                            if host != 'none' and host in instancesconfig[harvesterid] \
                                    and self.__str_to_bool( instancesconfig[harvesterid][host]['metrics']['lastsubmittedworker']['enable']):
                                timedelta_submitted = self.__get_timedelta(
                                    instancesconfig[harvesterid][host]['metrics']['lastsubmittedworker']['value'])
                                if lastsubmitedinstance[harvesterid]['harvesterhost'][host][
                                    'harvesterhostmaxtime'] < datetime.utcnow() - timedelta_submitted:
                                    error = "Last submitted worker was {0}".format(
                                        str(lastsubmitedinstance[harvesterid]['harvesterhost'][host][
                                            'harvesterhostmaxtime'])) + '\n'
                                    error_text.add(error)
                                    avaibility.append(0)
                            if harvesterid in metrics:
                                ### No heartbeat ###
                                heartbeattime = metrics[harvesterid][host].keys()[0]
                                contacts = instancesconfig[harvesterid][host]['contacts']
                                timedelta_heartbeat = self.__get_timedelta(instancesconfig[harvesterid][host]['metrics']['lastheartbeat']['value'])
                                if self.__str_to_bool( instancesconfig[harvesterid][host]['metrics']['lastheartbeat']['enable']) and \
                                        heartbeattime < datetime.utcnow() - timedelta_heartbeat:
                                    error = "Last heartbeat was {0}".format(
                                        str(heartbeattime)) + '\n'
                                    error_text.add(error)
                                    avaibility.append(0)

                            #### Metrics ####
                            memory = int(instancesconfig[harvesterid][host]['memory'])
                            cpu_warning = int(instancesconfig[harvesterid][host]['metrics']['cpu']['cpu_warning'])
                            cpu_critical = int(instancesconfig[harvesterid][host]['metrics']['cpu']['cpu_critical'])
                            disk_warning = int(instancesconfig[harvesterid][host]['metrics']['disk']['disk_warning'])
                            disk_critical = int(instancesconfig[harvesterid][host]['metrics']['disk']['disk_critical'])
                            memory_warning = int(instancesconfig[harvesterid][host]['metrics']['memory']['memory_warning'])
                            memory_critical = int(instancesconfig[harvesterid][host]['metrics']['memory']['memory_critical'])

                            cpu_enable = self.__str_to_bool(
                                    instancesconfig[harvesterid][host]['metrics']['cpu']['enable'])
                            disk_enable = self.__str_to_bool(
                                    instancesconfig[harvesterid][host]['metrics']['disk']['enable'])
                            memory_enable = self.__str_to_bool(
                                    instancesconfig[harvesterid][host]['metrics']['memory']['enable'])

                            #### Metrics DB ####
                            for metric in metrics[harvesterid][host][heartbeattime]:
                                #### CPU ####
                                if cpu_enable:
                                    cpu_pc = int(metric['cpu_pc'])
                                    if cpu_pc >= cpu_warning:
                                        avaibility.append(50)
                                        error = "Warning! CPU utilization:{0}".format(
                                            str(cpu_pc)) + '\n'
                                        error_text.add(error)
                                    elif cpu_pc >= cpu_critical:
                                        avaibility.append(10)
                                        error = "CPU utilization:{0}".format(
                                            str(cpu_pc)) + '\n'
                                        error_text.add(error)
                                #### Memory ####
                                if memory_enable:
                                    if 'memory_pc' in metric:
                                        memory_pc = int(metric['memory_pc'])
                                    else:
                                        memory_pc = int(self.__get_change(metric['rss_mib'], memory))
                                    if memory_pc >= memory_warning:
                                        avaibility.append(50)
                                        error = "Warning! Memory consumption:{0}".format(
                                            str(memory_pc)) + '\n'
                                        error_text.add(error)
                                    elif memory_pc >= memory_critical:
                                        avaibility.append(0)
                                        error = "Memory consumption:{0}".format(
                                            str(memory_pc)) + '\n'
                                        error_text.add(error)
                                #### HDD&HDD1  ####
                                if disk_enable:
                                    if 'volume_data_pc' in metric:
                                        volume_data_pc = int(metric['volume_data_pc'])
                                    else:
                                        volume_data_pc = -1
                                    if volume_data_pc >= disk_warning:
                                        avaibility.append(50)
                                        error = "Warning! Disk utilization:{0}".format(
                                                str(volume_data_pc)) + '\n'
                                        error_text.add(error)
                                    elif volume_data_pc >= disk_critical:
                                        avaibility.append(10)
                                        error = "Disk utilization:{0}".format(
                                                str(volume_data_pc)) + '\n'
                                        error_text.add(error)
                                    if 'volume_data1_pc' in metric:
                                        volume_data1_pc = int(metric['volume_data1_pc'])
                                        if volume_data1_pc >= disk_warning:
                                            avaibility.append(50)
                                            error = "Warning! Disk 1 utilization:{0}".format(
                                                    str(volume_data1_pc)) + '\n'
                                            error_text.add(error)
                                        elif volume_data1_pc >= disk_critical:
                                            avaibility.append(10)
                                            error = "Disk 1 utilization:{0}".format(
                                                str(volume_data1_pc)) + '\n'
                                            error_text.add(error)
                        try:
                            query = \
                            """insert into INSTANCES values ({0},{1},{2},{3},{4},{5},{6},{7},{8})""".format(str(harvesterid), str(host),
                                         str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                         heartbeattime, 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK')
                            cur.execute("insert into INSTANCES values (?,?,?,?,?,?,?,?,?)",
                                        (str(harvesterid), str(host),
                                         str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                         heartbeattime, 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK'))
                            connection.commit()
                            error_text = set()
                        except:
                            query = \
                                """UPDATE INSTANCES SET lastsubmitted = '{0}', active = {1}, availability = {2}, lastheartbeat = '{3}', contacts = '{4}', errorsdesc = '{5}' WHERE harvesterid = '{6}' and harvesterhost = '{7}'""".format(str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                        1, min(avaibility) if len(avaibility) > 0 else 100, heartbeattime, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK' , str(harvesterid),
                                        str(host))
                            cur.execute(query)
                            connection.commit()
                            error_text = set()
                            #log.info("The entry has been updated in cache by query ({0}".format(query))
                else:
                    cur.execute("DELETE FROM INSTANCES WHERE harvesterid = ?", [str(harvesterid)])
                    connection.commit()
        except ValueError as vex:
            _logger.error(vex.message)
            print vex.message
        except Exception as ex:
            _logger.error(ex.message)
            print ex.message

    # private method
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

    # private method
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

    # private method
    def __get_timedelta(self,time):
        if 'm' in time:
            return timedelta(minutes=int(time[:-1]))
        if 'h' in time:
            return timedelta(hours=int(time[:-1]))
        if 'd' in time:
            return timedelta(days=int(time[:-1]))
        else:
            return timedelta(minutes=int(time))