import xml.etree.ElementTree as ET
import os
from logger import ServiceLogger

log = ServiceLogger("configuration").logger

class Config:

    def __init__(self, path):
        self.XMLconfiguration = self.__read_configs_xml(path)

    #####private method####
    def __read_configs_xml(self, path):
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
                        configuration[harvesterid.attrib['harvesterid']]['instanceisenable'] = harvesterid.attrib['instanceisenable']
                        for hostlist in harvesterid:
                            for host in hostlist:
                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']] = {}
                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']]['hostisenable'] = host.attrib['hostisenable']
                                for hostparam in host:
                                    if hostparam.tag == 'contacts':
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = []
                                        for email in hostparam:
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag].append(
                                                email.text)
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = \
                                            ', '.join(
                                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag])
                                    elif hostparam.tag == 'metrics':
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag] = {}
                                        for metrics in hostparam:
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']] = {}
                                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                hostparam.tag][
                                                metrics.attrib['name']]['enable'] = metrics.attrib['enable']
                                            for metric in metrics:
                                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                                    hostparam.tag][
                                                    metrics.attrib['name']][metric.tag] = metric.text
                                    else:
                                        configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                            hostparam.tag] = hostparam.text

            log.debug(str(configuration))

            return configuration
        except Exception as ex:
            log.error(ex.message)
            print ex.message