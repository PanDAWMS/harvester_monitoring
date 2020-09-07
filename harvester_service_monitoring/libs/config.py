import xml.etree.ElementTree as ET
import os

from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from logger import ServiceLogger

_logger = ServiceLogger("configuration", __file__).logger


class Config:

    def __init__(self, path, type='hsm'):
        if type == 'hsm':
            self.XMLconfiguration = self.__read_harvester_configs_xml(path)
        elif type == 'schedd':
            self.XMLconfiguration = self.__read_schedd_configs_xml(path)

    #####private method####
    def __read_harvester_configs_xml(self, path):
        """
        Read harvester monitoring metrics from XML files
        """
        try:
            configuration = {}
            for file in os.listdir(path):
                if file.endswith(".xml"):
                    tree = ET.parse(os.path.join(path, file))
                    root = tree.getroot()
                    for harvesterid in root:
                        configuration[harvesterid.attrib['harvesterid']] = {}
                        configuration[harvesterid.attrib['harvesterid']]['instanceisenable'] = harvesterid.attrib[
                            'instanceisenable']
                        for hostlist in harvesterid:
                            for host in hostlist:
                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']] = {}
                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                    'hostisenable'] = host.attrib['hostisenable']
                                for hostparam in host:
                                    if hostparam.tag == 'contacts':
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                            hostparam.tag] = []
                                        for email in hostparam:
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag].append(
                                                email.text)
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                            hostparam.tag] = \
                                            ', '.join(
                                                configuration[harvesterid.attrib['harvesterid']][
                                                    host.attrib['hostname']][hostparam.tag])
                                    elif hostparam.tag == 'metrics':
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                            hostparam.tag] = {}
                                        # delay
                                        for delay in hostparam.attrib:
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag][delay] = hostparam.attrib[delay]
                                        for metrics in hostparam:
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']] = {}
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']]['enable'] = metrics.attrib['enable']
                                            for metric in metrics:
                                                configuration[harvesterid.attrib['harvesterid']][
                                                    host.attrib['hostname']][
                                                    hostparam.tag][
                                                    metrics.attrib['name']][metric.tag] = metric.text
                                    else:
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                            hostparam.tag] = hostparam.text

            _logger.debug(str(configuration))
            return configuration
        except Exception as ex:
            _logger.error(ex)
            print(ex)

    #####private method####
    def __read_schedd_configs_xml(self, path):
        """
        Read schedd monitoring metrics from XML files
        """
        try:
            configuration = {}
            for file in os.listdir(path):
                if file.endswith(".xml"):
                    tree = ET.parse(os.path.join(path, file))
                    root = tree.getroot()
                    for submissionhost in root:
                        configuration[submissionhost.attrib['hostname']] = {}
                        configuration[submissionhost.attrib['hostname']]['submissionhostenable'] = \
                        submissionhost.attrib['submissionhostenable']
                        for hostparam in submissionhost:
                            if hostparam.tag == 'contacts':
                                configuration[submissionhost.attrib['hostname']][hostparam.tag] = []
                                for email in hostparam:
                                    configuration[submissionhost.attrib['hostname']][hostparam.tag].append(
                                        email.text)
                                configuration[submissionhost.attrib['hostname']][hostparam.tag] = \
                                    ', '.join(
                                        configuration[submissionhost.attrib['hostname']][hostparam.tag])
                            elif hostparam.tag == 'metrics':
                                configuration[submissionhost.attrib['hostname']][hostparam.tag] = {}
                                # delay
                                for delay in hostparam.attrib:
                                    configuration[submissionhost.attrib['hostname']][
                                        hostparam.tag][delay] = hostparam.attrib[delay]
                                for metrics in hostparam:
                                    configuration[submissionhost.attrib['hostname']][
                                        hostparam.tag][
                                        metrics.attrib['name']] = {}
                                    configuration[submissionhost.attrib['hostname']][
                                        hostparam.tag][
                                        metrics.attrib['name']]['enable'] = metrics.attrib['enable']
                                    for metric in metrics:
                                        if metric.tag == 'process':
                                            if 'processlist' not in configuration[submissionhost.attrib['hostname']][
                                                hostparam.tag][metrics.attrib['name']]:
                                                configuration[submissionhost.attrib['hostname']][
                                                    hostparam.tag][
                                                    metrics.attrib['name']]['processlist'] = {}
                                            configuration[submissionhost.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']]['processlist'][metric.text] = metric.attrib[
                                                'enable']
                                        else:
                                            configuration[submissionhost.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']][metric.tag] = metric.text
                            else:
                                configuration[submissionhost.attrib['hostname']][
                                    hostparam.tag] = hostparam.text
            _logger.debug(str(configuration))
            return configuration
        except Exception as ex:
            _logger.error(ex.message)
            print (ex.message)
