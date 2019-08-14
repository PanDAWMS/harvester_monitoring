import sqlite3, re
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

    def get_data(self, type='hsm'):
        """
        Get instances/schedd info from SQLite cache
        """
        connection = self.connection

        if type == 'hsm':
            query = "SELECT * FROM INSTANCES"
        elif type == 'schedd':
            query = "SELECT * FROM SUBMISSIONHOSTS"
        data = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute(query)
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            object = dict(zip(columns, row))
            data.append(object)
        dataDict = {}
        if type == 'hsm':
            for instance in data:
                if instance['harvesterid'] not in dataDict:
                    dataDict[instance['harvesterid']] = {}
                    if instance['harvesterhost'] not in dataDict[instance['harvesterid']]:
                        dataDict[instance['harvesterid']][instance['harvesterhost']] = {
                            'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'],
                            'contacts': instance['contacts'].split(','),
                            'active': instance['active'], 'notificated': instance['notificated'], 'pravailability': instance['pravailability']}
                elif instance['harvesterid'] in dataDict:
                    if instance['harvesterhost'] not in dataDict[instance['harvesterid']]:
                        dataDict[instance['harvesterid']][instance['harvesterhost']] = {
                            'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'],
                            'contacts': instance['contacts'].split(','),
                            'active': instance['active'], 'notificated': instance['notificated'], 'pravailability': instance['pravailability']}
                        if 'none' in dataDict[instance['harvesterid']]:
                            del dataDict[instance['harvesterid']]['none']
            return dataDict
        elif type == 'schedd':
            for submissionhost in data:
                if submissionhost['submissionhost'] not in dataDict:
                    dataDict[submissionhost['submissionhost']] = {}
                    dataDict[submissionhost['submissionhost']] = {
                        'availability': submissionhost['availability'], 'errorsdesc': submissionhost['errorsdesc'],
                        'contacts': submissionhost['contacts'].split(','),
                        'active': submissionhost['active'], 'notificated': submissionhost['notificated'], 'pravailability': submissionhost['pravailability']}
                elif submissionhost['submissionhost'] in dataDict:
                    dataDict[submissionhost['submissionhost']] = {
                        'availability': submissionhost['availability'], 'errorsdesc': submissionhost['errorsdesc'],
                        'contacts': submissionhost['contacts'].split(','),
                        'active': submissionhost['active'], 'notificated': submissionhost['notificated'], 'pravailability': submissionhost['pravailability']}
                    if 'none' in dataDict[submissionhost['submissionhost']]:
                        del dataDict[submissionhost['submissionhost']]['none']
            return dataDict

    def get_history_logs(self, harvesterid, harvesterhost):
        """
        Get historylog from SQLite cache
        """
        connection = self.connection

        history = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute("SELECT checkmetrictime,notificationtime,availability,notificated,fulltext FROM HISTORYLOG "
                        "WHERE harvesterid = '{0}' and harvesterhost = '{1}'".format(
                harvesterid, harvesterhost))
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            object = dict(zip(columns, row))
            history.append(object)
        return history

    def get_checktime(self, harvesterid, harvesterhost, typeoferror, textoferror):
        """
        Get checkmetrictime from SQLite cache
        """
        connection = self.connection

        history = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute("SELECT checkmetrictime FROM HISTORYLOG "
                        "WHERE harvesterid = '{0}' and harvesterhost = '{1}' and typeoferror = '{2}' and textoferror = '{3}'".format(harvesterid, harvesterhost, typeoferror, textoferror))
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            history = dict(zip(columns, row))
            #history.append(object)
        return history

    def update_entry(self, field, table, value, harvesterid, harvesterhost, checkmetrictime=''):
        """
        Query update entry in SQLite cache
        :param conn: the Connection object
        :param field: column name in SQLite database
        :param value: value of the column name in SQLite database
        :param harvesterid: Harvester instance
        :param harvesterhost: Harvester host
        """
        try:
            connection = self.connection
            if checkmetrictime == '':
                query = """UPDATE {1} SET {0} = ?  WHERE harvesterid = ? and harvesterhost = ?""".format(table, field)
            else:
                query = """UPDATE {1} SET {0} = ?  WHERE harvesterid = ? and harvesterhost = ? and checkmetrictime = '{2}'""".format(table, field, checkmetrictime)
            cur = connection.cursor()

            cur.execute(query, (value,
                                harvesterid, harvesterhost))
            connection.commit()
            _logger.info("The {0} field was updated by the query '{1}'".format(field, query))
        except Exception as ex:
            _logger.error(ex.message)

    def __delete_history_availability(self, harvesterid, harvesterhost, typeoferror, textoferror):
        """
        Delete history in SQLite cache
        """
        connection = self.connection
        cur = connection.cursor()
        connection.row_factory = sqlite3.Row
        cur.execute(
            "DELETE FROM HISTORYLOG "
            "WHERE harvesterid = '{0}' and harvesterhost = '{1}' and typeoferror = '{2}' and textoferror = '{3}'".format(
                harvesterid, harvesterhost, typeoferror, textoferror))
        connection.commit()

    def update_previous_availability(self, harvesterid, harvesterhost, availability):
        """
        Update previous availability
        """
        connection = self.connection
        pravailability = availability

        instances = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            query = "SELECT pravailability FROM INSTANCES WHERE harvesterid = ? and harvesterhost = ?"

            cur.execute(query, (harvesterid, harvesterhost))

            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            pravailability = row[0]
        if pravailability != availability:
            try:
                connection = self.connection
                query = """UPDATE INSTANCES SET pravailability = ?  WHERE harvesterid = ? and harvesterhost = ?"""
                cur = connection.cursor()

                cur.execute(query, (pravailability,
                                harvesterid, harvesterhost))
                connection.commit()

                query = """UPDATE INSTANCES SET notificated = 0  WHERE harvesterid = ? and harvesterhost = ?"""
                cur = connection.cursor()

                cur.execute(query, (harvesterid, harvesterhost))
                connection.commit()

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
                                    self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                     typeoferror='critical', textoferror='submitted',
                                                                       availability=0, notificated=0, fulltext=error)
                                else:
                                    self.__delete_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                     typeoferror='critical', textoferror='submitted')
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
                                    self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                     typeoferror='critical', textoferror='heartbeat',
                                                                       availability=0, notificated=0, fulltext=error)
                                else:
                                    self.__delete_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                     typeoferror='critical', textoferror='heartbeat')

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
                                    if cpu_pc >= cpu_warning and cpu_pc < cpu_critical:
                                        avaibility.append(50)
                                        error = "Warning! CPU utilization:{0}".format(
                                            str(cpu_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                    typeoferror='warning', textoferror='cpu' ,
                                                                           availability=50, notificated=0, fulltext=error)
                                    elif cpu_pc >= cpu_critical:
                                        avaibility.append(10)
                                        error = "CPU utilization:{0}".format(
                                            str(cpu_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                         typeoferror='critical', textoferror='cpu',
                                                                           availability=10, notificated=0, fulltext=error)
                                #### Memory ####
                                if memory_enable:
                                    if 'memory_pc' in metric:
                                        memory_pc = int(metric['memory_pc'])
                                    else:
                                        memory_pc = int(self.__get_change(metric['rss_mib'], memory))
                                    if memory_pc >= memory_warning and memory_pc < memory_critical:
                                        avaibility.append(50)
                                        error = "Warning! Memory consumption:{0}".format(
                                            str(memory_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                         typeoferror='warning', textoferror= 'memory',
                                                                           availability=50, notificated=0, fulltext=error)
                                    elif memory_pc >= memory_critical:
                                        avaibility.append(10)
                                        error = "Memory consumption:{0}".format(
                                            str(memory_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                         typeoferror='critical', textoferror= 'memory',
                                                                           availability=10, notificated=0, fulltext=error)
                                #### HDD&HDD1  ####
                                if disk_enable:
                                    if 'volume_data_pc' in metric:
                                        volume_data_pc = int(metric['volume_data_pc'])
                                    else:
                                        volume_data_pc = -1
                                    if volume_data_pc >= disk_warning and volume_data_pc < disk_critical:
                                        avaibility.append(50)
                                        error = "Warning! Disk utilization:{0}".format(
                                                str(volume_data_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                         typeoferror='warning', textoferror= 'disk',
                                                                           availability=50, notificated=0, fulltext=error)
                                    elif volume_data_pc >= disk_critical:
                                        avaibility.append(10)
                                        error = "Disk utilization:{0}".format(
                                                str(volume_data_pc)) + '\n'
                                        error_text.add(error)
                                        self.__insert_history_availability(harvesterid=harvesterid, harvesterhost=host,
                                                                         typeoferror='critical', textoferror='disk',
                                                                           availability=10, notificated=0, fulltext=error)
                                    if 'volume_data1_pc' in metric:
                                        volume_data1_pc = int(metric['volume_data1_pc'])
                                        if volume_data1_pc >= disk_warning and volume_data1_pc < disk_critical:
                                            avaibility.append(50)
                                            error = "Warning! Disk 1 utilization:{0}".format(
                                                    str(volume_data1_pc)) + '\n'
                                            error_text.add(error)
                                            self.__insert_history_availability(harvesterid=harvesterid,
                                                                               harvesterhost=host,
                                                                               typeoferror='warning',
                                                                               textoferror='disk1', availability=50,
                                                                               notificated=0,
                                                                               fulltext=error)
                                        elif volume_data1_pc >= disk_critical:
                                            avaibility.append(10)
                                            error = "Disk 1 utilization:{0}".format(
                                                str(volume_data1_pc)) + '\n'
                                            error_text.add(error)
                                            self.__insert_history_availability(harvesterid=harvesterid,
                                                                               harvesterhost=host,
                                                                               typeoferror='critical',
                                                                               textoferror='disk1', availability=10,
                                                                               fulltext=error)
                        try:
                            query = \
                            """insert into INSTANCES values ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9})""".format(str(harvesterid), str(host),
                                         str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                         heartbeattime, 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK', min(avaibility) if len(avaibility) > 0 else 100)

                            cur.execute("insert into INSTANCES values (?,?,?,?,?,?,?,?,?,?)",
                                        (str(harvesterid), str(host),
                                         str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                         heartbeattime, 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK', min(avaibility) if len(avaibility) > 0 else 100))
                            connection.commit()
                            error_text = set()
                        except:
                            avaibility = min(avaibility) if len(avaibility) > 0 else 100
                            query = \
                                """UPDATE INSTANCES SET lastsubmitted = '{0}', active = {1}, availability = {2}, lastheartbeat = '{3}', contacts = '{4}', errorsdesc = '{5}' WHERE harvesterid = '{6}' and harvesterhost = '{7}'""".format(str(lastsubmitedinstance[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                        1, avaibility, heartbeattime, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK', str(harvesterid),
                                        str(host))
                            cur.execute(query)
                            self.update_previous_availability(str(harvesterid), str(host), avaibility)
                            connection.commit()

                            error_text = set()
                            #logger.info("The entry has been updated in cache by query ({0}".format(query))
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
    def __insert_history_availability(self, harvesterid, harvesterhost, typeoferror, textoferror,
                                      availability, notificated, fulltext):
        """
        Write history of errors to SQLite cache
        """
        connection = self.connection
        cur = connection.cursor()
        connection.row_factory = sqlite3.Row

        history = self.get_checktime(harvesterid, harvesterhost, typeoferror, textoferror)

        if len(history)>0:
            ckeckmetrictime = datetime.strptime(history['checkmetrictime'], "%Y-%m-%d %H:%M:%S.%f")
            if typeoferror == 'warning' and textoferror not in ['heartbeat', 'submitted']:
                if 'warning_delay' in self.instancesconfigs[harvesterid][harvesterhost]['metrics']:
                    warning_delay = self.__get_timedelta(self.instancesconfigs[harvesterid][harvesterhost]['metrics']['warning_delay'])
                else:
                    warning_delay = self.__get_timedelta('6h')
                if ckeckmetrictime < datetime.utcnow() - warning_delay:
                    cur.execute(
                        "DELETE FROM HISTORYLOG "
                        "WHERE harvesterid = '{0}' and harvesterhost = '{1}' and typeoferror = '{2}' and textoferror = '{3}'".format(
                            harvesterid, harvesterhost, typeoferror, textoferror))
                    connection.commit()
            elif typeoferror == 'critical' and textoferror not in ['heartbeat', 'submitted']:
                if 'critical_delay' in self.instancesconfigs[harvesterid][harvesterhost]['metrics']:
                    critical_delay = self.__get_timedelta(self.instancesconfigs[harvesterid][harvesterhost]['metrics']['critical_delay'])
                else:
                    critical_delay = self.__get_timedelta('6h')
                if ckeckmetrictime < datetime.utcnow() - critical_delay:
                    cur.execute(
                        "DELETE FROM HISTORYLOG "
                        "WHERE harvesterid = '{0}' and harvesterhost = '{1}' and typeoferror = '{2}' and textoferror = '{3}'".format(
                            harvesterid, harvesterhost, typeoferror, textoferror))
                    connection.commit()
        try:
            cur.execute(
                "insert into HISTORYLOG (harvesterid,harvesterhost,checkmetrictime,typeoferror,textoferror,availability,notificated,fulltext) values (?,?,?,?,?,?,?,?)",
                    (harvesterid, harvesterhost, str(datetime.utcnow()), typeoferror, textoferror, availability,
                        notificated, fulltext))
            connection.commit()
        except:
            pass
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
    # SCHEDD Cache
    def scheddhosts_availability(self, metrics):
        """
        Check submissionhost for availability and update SQLite cache
        """
        try:

            connection = self.connection
            schedd_configs = self.instancesconfigs

            cur = connection.cursor()
            schedd_hosts = schedd_configs.keys()
            connection.row_factory = sqlite3.Row

            for host in schedd_hosts:
                error_text = set()

                submissionhostisenable = self.__str_to_bool(schedd_configs[host]['submissionhostenable'])
                del schedd_configs[host]['submissionhostenable']
                ### Instance is enable ###
                if submissionhostisenable:
                    avaibility = []

                    ### No submitted worker ###
                    timedelta_submitted = timedelta(minutes=30)
                    if host != 'none' \
                            and self.__str_to_bool(schedd_configs[host]['metrics']['lastsubmittedworker']['enable']):
                        timedelta_submitted = self.__get_timedelta(
                            schedd_configs[host]['metrics']['lastsubmittedworker']['value'])
                        if metrics[host]['last_submittime'] < datetime.utcnow() - timedelta_submitted:
                            error = "Last submitted worker was {0}".format(
                                str(metrics[host]['last_submittime'])) + '\n'
                            error_text.add(error)
                            avaibility.append(0)
                            self.__insert_schedd_history_availability(submissionhost=host,
                                                             typeoferror='critical', textoferror='submitted',
                                                               availability=0, notificated=0, fulltext=error)
                        else:
                            self.__delete_schedd_history_availability(submissionhost=host,
                                                             typeoferror='critical', textoferror='submitted')
                    contacts = schedd_configs[host]['contacts']
                    #### Metrics ####
                    disk_warning = int(schedd_configs[host]['metrics']['disk']['disk_warning'])
                    disk_critical = int(schedd_configs[host]['metrics']['disk']['disk_critical'])

                    processes = schedd_configs[host]['metrics']['processes']['processlist']

                    disk_enable = self.__str_to_bool(
                        schedd_configs[host]['metrics']['disk']['enable'])
                    processes_enable = self.__str_to_bool(
                        schedd_configs[host]['metrics']['processes']['enable'])
                    #### Metrics DB ####
                    for metric in metrics[host]:
                        #### DISK  ####
                        if disk_enable and metric.startswith('disk_usage'):
                            disk_pc = int(metrics[host][metric])
                            if disk_pc >= disk_warning and disk_pc < disk_critical:
                                avaibility.append(50)
                                error = "Warning! Disk {0} utilization:{1}".format(
                                    metric.replace('disk_usage_',''), str(disk_pc)) + '\n'
                                error_text.add(error)
                                self.__insert_schedd_history_availability(submissionhost=host,
                                                                 typeoferror='warning', textoferror=metric,
                                                                   availability=50, notificated=0, fulltext=error)
                            elif disk_pc >= disk_critical:
                                avaibility.append(10)
                                error = "Disk {0} utilization:{1}".format(
                                    metric.replace('disk_usage_',''), str(disk_pc)) + '\n'
                                error_text.add(error)
                                self.__insert_schedd_history_availability(submissionhost=host,
                                                                 typeoferror='critical', textoferror=metric,
                                                                   availability=10, notificated=0, fulltext=error)
                        if processes_enable:
                            if any(key in metric and bool(value) for key,value in processes.items()):
                                if metrics[host][metric] != 100:
                                    avaibility.append(10)
                                    error = "{0} process not found".format(
                                        metric) + '\n'
                                    error_text.add(error)
                                    self.__insert_schedd_history_availability(submissionhost=host,
                                                                              typeoferror='critical',
                                                                              textoferror=metric,
                                                                              availability=10, notificated=0,
                                                                              fulltext=error)


                    try:
                        query = \
                        """insert into SUBMISSIONHOSTS values ({0},{1},{2},{3},{4},{5},{6},{7},{8})""".format(str(host),
                                     str(metrics[host]['last_submittime']),
                                     str(metrics[host]['creation_time']), 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK', min(avaibility) if len(avaibility) > 0 else 100)

                        cur.execute("insert into SUBMISSIONHOSTS values (?,?,?,?,?,?,?,?,?)",
                                    (str(host),
                                     str(metrics[host]['last_submittime']),
                                     str(metrics[host]['creation_time']), 1, 0, min(avaibility) if len(avaibility) > 0 else 100, str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK', min(avaibility) if len(avaibility) > 0 else 100))
                        connection.commit()
                        error_text = set()
                    except:
                        avaibility = min(avaibility) if len(avaibility) > 0 else 100
                        query = \
                            """UPDATE SUBMISSIONHOSTS SET lastsubmitted = '{0}', active = {1}, availability = {2}, lastheartbeat = '{3}', contacts = '{4}', errorsdesc = '{5}' WHERE submissionhost = '{6}'""".format(str(metrics[host]['last_submittime']),
                                    1, avaibility, str(metrics[host]['creation_time']), str(contacts), ''.join(str(e) for e in error_text) if len(error_text) > 0 else 'service metrics OK',
                                    str(host))
                        cur.execute(query)
                        self.update_schedd_previous_availability(str(host), avaibility)
                        connection.commit()

                        error_text = set()
                        #logger.info("The entry has been updated in cache by query ({0}".format(query))
                else:
                    cur.execute("DELETE FROM SUBMISSIONHOSTS WHERE submissionhost = ?", [str(host)])
                    connection.commit()
        except ValueError as vex:
            _logger.error(vex.message)
            print vex.message
        except Exception as ex:
            _logger.error(ex.message)
            print ex.message

    def get_schedd_history_logs(self, submissionhost):
        """
        Get schedd historylog from SQLite cache
        """
        connection = self.connection

        history = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute("SELECT checkmetrictime,notificationtime,availability,notificated,fulltext FROM SCHEDDHISTORYLOG "
                        "WHERE submissionhost = '{0}'".format(
                submissionhost))
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            object = dict(zip(columns, row))
            history.append(object)
        return history

    def get_schedd_checktime(self, submissionhost, typeoferror, textoferror):
        """
        Get schedd checkmetrictime from SQLite cache
        """
        connection = self.connection

        history = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            cur.execute("SELECT checkmetrictime FROM SCHEDDHISTORYLOG "
                        "WHERE submissionhost = '{0}' and typeoferror = '{1}' and textoferror = '{2}'".format(submissionhost,  typeoferror, textoferror))
            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            history = dict(zip(columns, row))
            #history.append(object)
        return history

    def update_schedd_entry(self, field, table, value, submissionhost, checkmetrictime=''):
        """
        Query update entry in SQLite cache for shedd host
        :param conn: the Connection object
        :param field: column name in SQLite database
        :param value: value of the column name in SQLite database
        :param submissionhost: Harvester submissionhost
        """
        try:
            connection = self.connection
            if checkmetrictime == '':
                query = """UPDATE {1} SET {0} = ?  WHERE submissionhost = ?""".format(table, field)
            else:
                query = """UPDATE {1} SET {0} = ?  WHERE submissionhost = ? and checkmetrictime = '{2}'""".format(table, field, checkmetrictime)
            cur = connection.cursor()

            cur.execute(query, (value,
                                submissionhost))
            connection.commit()
            _logger.info("The {0} field was updated by the query '{1}'".format(field, query))
        except Exception as ex:
            _logger.error(ex.message)

    def __delete_schedd_history_availability(self, submissionhost, typeoferror, textoferror):
        """
        Delete schedd history in SQLite cache
        """
        connection = self.connection
        cur = connection.cursor()
        connection.row_factory = sqlite3.Row
        cur.execute(
            "DELETE FROM SCHEDDHISTORYLOG "
            "WHERE submissionhost = '{0}' and typeoferror = '{1}' and textoferror = '{2}'".format(
                submissionhost, typeoferror, textoferror))
        connection.commit()
   # private method
    def __insert_schedd_history_availability(self, submissionhost, typeoferror, textoferror,
                                      availability, notificated, fulltext):
        """
        Write schedd history of errors to SQLite cache
        """
        connection = self.connection
        cur = connection.cursor()
        connection.row_factory = sqlite3.Row

        history = self.get_schedd_checktime(submissionhost, typeoferror, textoferror)

        if len(history)>0:
            ckeckmetrictime = datetime.strptime(history['checkmetrictime'], "%Y-%m-%d %H:%M:%S.%f")
            if typeoferror == 'warning' and textoferror not in ['heartbeat', 'submitted']:
                if 'warning_delay' in self.instancesconfigs[submissionhost]['metrics']:
                    warning_delay = self.__get_timedelta(self.instancesconfigs[submissionhost]['metrics']['warning_delay'])
                else:
                    warning_delay = self.__get_timedelta('6h')
                if ckeckmetrictime < datetime.utcnow() - warning_delay:
                    cur.execute(
                        "DELETE FROM SCHEDDHISTORYLOG "
                        "WHERE submissionhost = '{0}' and typeoferror = '{1}' and textoferror = '{2}'".format(
                            submissionhost, typeoferror, textoferror))
                    connection.commit()
            elif typeoferror == 'critical' and textoferror not in ['heartbeat', 'submitted']:
                if 'critical_delay' in self.instancesconfigs[submissionhost]['metrics']:
                    critical_delay = self.__get_timedelta(self.instancesconfigs[submissionhost]['metrics']['critical_delay'])
                else:
                    critical_delay = self.__get_timedelta('6h')
                if ckeckmetrictime < datetime.utcnow() - critical_delay:
                    cur.execute(
                        "DELETE FROM SCHEDDHISTORYLOG "
                        "WHERE submissionhost = '{0}' and typeoferror = '{1}' and textoferror = '{2}'".format(
                            submissionhost, typeoferror, textoferror))
                    connection.commit()
        try:
            cur.execute(
                "insert into SCHEDDHISTORYLOG (submissionhost,checkmetrictime,typeoferror,textoferror,availability,notificated,fulltext) values (?,?,?,?,?,?,?)",
                    (submissionhost, str(datetime.utcnow()), typeoferror, textoferror, availability,
                        notificated, fulltext))
            connection.commit()
        except:
            pass

    def update_schedd_previous_availability(self, submissionhost, availability):
        """
        Update previous availability for shedd host
        """
        connection = self.connection
        pravailability = availability

        instances = []
        try:
            connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            query = "SELECT pravailability FROM SUBMISSIONHOSTS WHERE submissionhost = ?"

            cur.execute(query, (submissionhost,))

            rows = cur.fetchall()
        except sqlite3.Error as ex:
            _logger.error(ex.message)
        columns = [str(i[0]).lower() for i in cur.description]
        for row in rows:
            pravailability = row[0]
        if pravailability != availability:
            try:
                connection = self.connection
                query = """UPDATE SUBMISSIONHOSTS SET pravailability = ?  WHERE submissionhost = ?"""
                cur = connection.cursor()

                cur.execute(query, (pravailability,
                                    submissionhost))
                connection.commit()

                query = """UPDATE SUBMISSIONHOSTS SET notificated = 0  WHERE submissionhost = ?"""
                cur = connection.cursor()

                cur.execute(query, (submissionhost))
                connection.commit()
            except Exception as ex:
                _logger.error(ex.message)