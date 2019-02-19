import xml.etree.ElementTree as ET

class Config:

    def __init__(self, path):
        self.XMLconfiguration = self.__read_config_xml(path)

    "private method"
    def __read_config_xml(self, path):
        """
        Rea harvester monitoring metrics from XML file
        """
        configuration = {}
        tree = ET.parse(path)
        root = tree.getroot()

        for harvesterid in root:
            configuration[harvesterid.attrib['harvesterid']] = {}
            configuration[harvesterid.attrib['harvesterid']]['instanceisenable'] = harvesterid.attrib['instanceisenable']
            for hostlist in harvesterid:
                for host in hostlist:
                    configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']] = {}
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
                            for metric in hostparam:
                                configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][hostparam.tag][
                                    metric.tag] \
                                    = int(metric.text)
                        else:
                            configuration[harvesterid.attrib['harvesterid']][host.attrib['hostname']][
                                hostparam.tag] = hostparam.text
        return configuration