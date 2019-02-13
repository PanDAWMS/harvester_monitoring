import smtplib
import urllib2
import re, os

import sqlite3
import json

import cx_Oracle

from datetime import datetime, timedelta

from elasticsearch import Elasticsearch
from configparser import ConfigParser
from elasticsearch_dsl import Search, A

from cernservicexml import ServiceDocument, XSLSPublisher

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def create_es_connection(path, use_ssl=True, verify_certs=False, timeout=30, max_retries=10,
                       retry_on_timeout=True):
    """ Create a database connection to ElasticSearch
    :param
    :return: Connection object or None
    """
    try:
        cfg = ConfigParser()
        cfg.read(path)
        eslogin = cfg.get('server', 'login')
        espasswd = cfg.get('server', 'password')
        host = cfg.get('server', 'host')
        port = cfg.get('server', 'port')
    except:
        pass
    try:
        connection = Elasticsearch(
            [{'host': host, 'port': int(port)}],
            http_auth=(eslogin, espasswd),
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
        )
        return connection
    except:
        pass

    return None


def create_pandadb_connection(path):
    """ Create a database connection to the PanDA database
    :param : database file
    :return: Connection object or None
    """
    try:

        cfg = ConfigParser()
        cfg.read(path)
        dbuser = cfg.get('pandadb', 'login')
        dbpasswd = cfg.get('pandadb', 'password')
        description = cfg.get('pandadb', 'description')
    except:
        pass
    try:
        connection = cx_Oracle.connect(dbuser, dbpasswd, description)
        return connection
    except:
        pass
    return None

def create_sqlite_connection(db_file):
    """ Create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None

def get_last_submittedtime(conn):
    """
    :param conn: connection object
    :return: List with last date of submitted workers for each harvester instance and list with last date of submitted workers for each harvester host
    """
    s = Search(using=conn, index='atlas_harvesterworkers')[:0]


    s.aggs.bucket('harvesterid', 'terms', field='harvesterid.keyword', size=1000) \
        .metric('max_submittime', 'max', field='submittime') \
        .metric('min_submittime', 'min', field='submittime') \
        .bucket('harvesterhost', 'terms', field='harvesterhost.keyword', order={'max_hostsubmittime': 'desc'}, size=100) \
        .metric('max_hostsubmittime', 'max', field='submittime')

    s = s.execute()

    harvesterhostDict = {}
    harvesteridDict = {}

    for hit in s.aggregations.harvesterid:
        harvesteridDict[hit.key] = {
            'max': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z'),
            'min': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z')
        }
        if len(hit.harvesterhost) == 0:
            harvesteridDict[hit.key]['harvesterhost'] = {}
            harvesteridDict[hit.key]['harvesterhost']['none'] = {
                'harvesterhostmaxtime': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z'),
                'harvesterhostmintime': datetime.strptime(hit.max_submittime.value_as_string, '%Y-%m-%dT%H:%M:%S.000Z')}
        for harvesterhost in hit.harvesterhost:
            if 'harvesterhost' not in harvesteridDict[hit.key]:
                harvesteridDict[hit.key]['harvesterhost'] = {}
            harvesteridDict[hit.key]['harvesterhost'][harvesterhost.key] = {
                'harvesterhostmaxtime': datetime.strptime(harvesterhost.max_hostsubmittime.value_as_string,
                                                          '%Y-%m-%dT%H:%M:%S.000Z')}
    return harvesterhostDict, harvesteridDict

def get_exceptions(conn):
    """
    Description
    :param :
    """
    s = Search(using=conn, index='atlas_harvesterlogs-*').filter('range', **{
        '@timestamp': {'gte': 'now-30m', 'lte': 'now'}}).filter('terms', tags=['error'])
    s = s.scan()
    return None

def send_notification_email(mailserv="localhost", fromemail="noreply@mail.cern.ch", to=['aaleksee@cern.ch'], text='', subject=''):
    """
    description
    :param :
    """
    SERVER = mailserv
    FROM = fromemail
    TO = to
    SUBJECT = subject
    TEXT = text

    message = """\
From: %s
To: %s
Subject: %s

%s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

    server = smtplib.SMTP(SERVER)
    server.sendmail(FROM, TO, message)
    server.quit()

def get_all_instances(conn):
    """
    Query all rows in the instances table in SQLListe database
    :param conn: connection object
    :return:
    """
    harvesterList = []
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM INSTANCES")
    rows = cur.fetchall()
    columns = [str(i[0]).lower() for i in cur.description]

    for row in rows:
        object = dict(zip(columns, row))
        harvesterList.append(object)

    return harvesterList

def instance_status(conn, lastsubmited, instancesconfig, metrics):
    """
    Description
    :param conn: the Connection object:
    """
    cur = conn.cursor()
    harvesters = instancesconfig.keys()
    conn.row_factory = sqlite3.Row
    for harvesterid in harvesters:
        avaibility = 100
        errorsdesc = ''
        instanceisenable = str_to_bool(instancesconfig[harvesterid]['instanceisenable'])
        del instancesconfig[harvesterid]['instanceisenable']
        ### Instance is enable ###
        if instanceisenable:
            ### No submitted worker ###
            for host in lastsubmited[harvesterid]['harvesterhost']:
                mins = 30
                if host != 'none' and host in instancesconfig[harvesterid]:
                    mins = int(instancesconfig[harvesterid][host]['lastsubmittedworker'])
                    if lastsubmited[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime'] < datetime.utcnow() - timedelta(
                        minutes=mins):
                        errorsdesc = errorsdesc + "Last submitted worker was {0}".format(str(lastsubmited[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime'])) + '\n'
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
                            free_mib = int(get_change(metric['rss_mib'], memory))
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
                                 str(lastsubmited[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                 heartbeattime, 1, 0, avaibility, str(contacts),str(errorsdesc)))
                    conn.commit()
                except:
                    query = \
                    """UPDATE INSTANCES 
SET lastsubmitted = '{0}', active = {1}, availability = {2}, lastheartbeat = '{3}', contacts = '{4}', errorsdesc = '{5}'
WHERE harvesterid = '{6}' and harvesterhost = '{7}'
                    """.format(str(lastsubmited[harvesterid]['harvesterhost'][host]['harvesterhostmaxtime']),
                                   1, avaibility,heartbeattime, str(contacts), str(errorsdesc), str(harvesterid), str(host))

                    cur.execute(query)
                    conn.commit()
        else:
            cur.execute("DELETE FROM INSTANCES WHERE harvesterid = ?", [str(harvesterid)])
            conn.commit()

def get_change(current, previous):
    """
    Description
    :param
    :return:
    """
    current = float(current)
    previous = float(previous)
    if current == previous:
        return 0
    try:
        r = (abs(current - previous)/previous) * 100
        if r > 100:
            r = 100
        return round(r)
    except ZeroDivisionError:
        return 100

def get_last_metrics(connection):
    """
    Description
    :param connection: cnnection object:
    """
    metrcis = {}
    query = """SELECT t.harvester_id, t.harvester_host, t.CREATION_TIME, t.METRICS FROM ( 
    SELECT harvester_id, harvester_host, MAX(creation_time) AS CREATION_TIME
    FROM atlas_panda.harvester_metrics
    GROUP BY harvester_id, harvester_host) x 
    JOIN atlas_panda.harvester_metrics t ON x.harvester_id =t.harvester_id
    AND x.harvester_host = t.harvester_host AND x.CREATION_TIME = t.CREATION_TIME"""

    results = read_query(connection, query)
    for result in results:
        if result['harvester_id'] not in metrcis:
            metrcis[result['harvester_id']] = {}
        if result['harvester_host'] not in metrcis[result['harvester_id']]:
            metrcis[result['harvester_id']][result['harvester_host']] = {}
        metrcis[result['harvester_id']][result['harvester_host']].setdefault(result['creation_time'],[]).append(json.loads(result['metrics']))

    return metrcis

def str_to_bool(s):
    """
    Description
    :param s: :
    """
    if s == 'True':
         return True
    elif s == 'False':
         return False
    else:
         raise ValueError

def update_field(conn, field, value, harvesterid, harvesterhost):
    """
    Query update field in SQLite database
    :param conn: the Connection object
    :param field: column name in SQLite database
    :param value: value of the column name in SQLite database
    :param harvesterid: Harvester instance
    :param harvesterhost: Harvester host
    """
    query = """UPDATE INSTANCES SET {0} = ?  WHERE harvesterid = ? and harvesterhost = ?""".format(field)
    cur = conn.cursor()

    cur.execute(query, (value,
                harvesterid, harvesterhost))
    conn.commit()

def read_query(connection, query):
    """
    Description
    :param connection:
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return rows_to_dict_list(cursor)
    finally:
        if cursor is not None:
            cursor.close()

def rows_to_dict_list(cursor):
    """
    Description
    :param cursor: cursor object
    """
    columns = [str(i[0]).lower() for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]

def read_config():
    """
    Description
    :return: Dictionary of parameters for monitoring
    """
    conf = {}
    import xml.etree.ElementTree as ET
    tree = ET.parse(BASE_DIR +'/configuration/grid.xml')
    root = tree.getroot()

    for harvesterid in root:
        conf[harvesterid.attrib['harvesterid']] = {}
        conf[harvesterid.attrib['harvesterid']]['instanceisenable'] = harvesterid.attrib['instanceisenable']
        for hostlist in harvesterid:
            for host in hostlist:
                conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']] = {}
                for hostparam in host:
                    if hostparam.tag == 'contacts':
                        conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = []
                        for email in hostparam:
                            conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag].append(email.text)
                        conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = \
                            ', '.join(conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag])
                    elif hostparam.tag == 'metrics':
                        conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = {}
                        for metric in hostparam:
                            conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag][metric.tag]\
                                = int(metric.text)
                    else:
                        conf[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = hostparam.text
    return conf

def main():

    instancesconfig = read_config()
    sqlLiteconnection = create_sqlite_connection(db_file=BASE_DIR+'/storage/harvester.db')
    pandaDBconnection = create_pandadb_connection(path=BASE_DIR+'/config.ini')
    metrics = get_last_metrics(pandaDBconnection)
    esconnection = create_es_connection(path=BASE_DIR+'/settings.ini')
    dictHarvesterHosts, dictHarvesterInstnaces = get_last_submittedtime(esconnection)
    instance_status(sqlLiteconnection, dictHarvesterInstnaces, instancesconfig, metrics)
    instances = get_all_instances(sqlLiteconnection)
    ### instances without hosts ###
    instancesNoneList = {}

    for instance in instances:
        if instance['harvesterid'] not in instancesNoneList:
            instancesNoneList[instance['harvesterid']] = {}
            if instance['harvesterhost'] not in instancesNoneList[instance['harvesterid']]:
                instancesNoneList[instance['harvesterid']][instance['harvesterhost']] = {
                    'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'], 'contacts': instance['contacts'].split(','),
                    'active': instance['active'], 'notificated': instance['notificated']}
        elif instance['harvesterid'] in instancesNoneList:
            if instance['harvesterhost'] not in instancesNoneList[instance['harvesterid']]:
                instancesNoneList[instance['harvesterid']][instance['harvesterhost']] = {
                    'availability': instance['availability'], 'errorsdesc': instance['errorsdesc'], 'contacts': instance['contacts'].split(','),
                    'active': instance['active'], 'notificated': instance['notificated']}
                if 'none' in instancesNoneList[instance['harvesterid']]:
                    del instancesNoneList[instance['harvesterid']]['none']

    for instance in instancesNoneList:
        for harvesterhost in instancesNoneList[instance]:
            if harvesterhost != 'none':
                availability = instancesNoneList[instance][harvesterhost]['availability']
                notificated = instancesNoneList[instance][harvesterhost]['notificated']
                contacts = instancesNoneList[instance][harvesterhost]['contacts']
                text = instancesNoneList[instance][harvesterhost]['errorsdesc']
                if (availability == 0 or availability == 10) and notificated == 0:
                    send_notification_email(text=text,
                              subject='Service issues on {0} {1}'.format(instance, harvesterhost),
                              to=contacts
                              )
                    update_field(sqlLiteconnection, 'notificated', 1, instance, harvesterhost)
                elif availability == 100 and notificated == 1:
                    update_field(sqlLiteconnection,'notificated', 0, instance, harvesterhost)
                host = harvesterhost.split('.')[0]
                doc = {}
                doc = ServiceDocument('harv_{0}_{1}'.format(instance, host), availability=availability, contact=','.join(contacts), availabilitydesc="PandaHarvester instance:{0}".format(instance), availabilityinfo="{0}".format(text))
                XSLSPublisher.send(doc)

if __name__ == "__main__":
    main()