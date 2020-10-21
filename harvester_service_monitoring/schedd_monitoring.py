import os
import subprocess
from datetime import datetime

from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from libs.config import Config
from libs.sqlite_cache import Sqlite
from libs.es import Es
from libs.notifications import Notifications
from libs.kibanaXML import xml_doc
from libs.kibanaXSLS import SlsDocument

from logger import ServiceLogger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_logger = ServiceLogger("schedd_monitoring", __file__).logger


def main():
    config = Config(BASE_DIR + '/schedd_configuration/', type='schedd')
    sqlite = Sqlite(BASE_DIR + '/storage/hsm.db', config.XMLconfiguration)

    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    es = Es(settings)

    schedd_metrics = es.get_schedd_metrics()
    sqlite.scheddhosts_availability(schedd_metrics)
    #
    submissionhosts = sqlite.get_data(type='schedd')

    for host in submissionhosts:
        kibana_xml = xml_doc()
        sls_doc = SlsDocument()

        if host != 'none':
            availability = submissionhosts[host]['availability']
            notificated = submissionhosts[host]['notificated']
            contacts = submissionhosts[host]['contacts']
            text = submissionhosts[host]['errorsdesc']
            errors = sqlite.get_schedd_history_logs(submissionhost=host)

            if availability == 0 or availability == 10 or availability == 50:
                if len(errors) > 0:
                    mailtext = ''
                    for error in errors:
                        if error['notificated'] == 0:
                            mailtext = mailtext + error['fulltext']
                            sqlite.update_schedd_entry('SCHEDDHISTORYLOG', 'notificated', 1, host,
                                                       checkmetrictime=error['checkmetrictime'])
                            sqlite.update_schedd_entry('SCHEDDHISTORYLOG', 'notificationtime', str(datetime.utcnow()),
                                                       host,
                                                       checkmetrictime=error['checkmetrictime'])
                    if mailtext != '':
                        email = Notifications(text=mailtext,
                                              subject='Service issues on submissionhost: {0}'.format(host),
                                              to=contacts)
                        email.send_notification_email()
                        sqlite.update_schedd_entry('SUBMISSIONHOSTS', 'notificated', 1, host)
                        email = {}
            elif availability == 100 and notificated == 1:
                sqlite.update_schedd_entry('SUBMISSIONHOSTS', 'notificated', 0, host)

            id = 'PandaHarvesterCondor'

            kibana_xml.set_id('%s_%s' % (id, (str(host).split('.'))[0]))
            kibana_xml.set_availability(str(availability))
            kibana_xml.set_status(availability)
            kibana_xml.set_avail_desc(host)
            kibana_xml.set_avail_info(text)

            try:
                tmp_xml = kibana_xml.print_xml('status')
                file_name = '%s/xml/%s_%s_kibana.xml' % (
                BASE_DIR, 'PandaHarvesterCondor', (str(host).split('.'))[0])
                tmp_file = open(file_name, 'w')
                tmp_file.write(tmp_xml)
                tmp_file.close()
                subprocess.call(["curl", "-F", "file=@%s" % file_name, 'xsls.cern.ch'])
            except Exception as ex:
                _logger.error(ex)
                print(ex)

            sls_doc.set_id('%s_%s' % (id, (str(host).split('.'))[0]))
            sls_doc.set_status(availability)
            sls_doc.set_avail_desc(host)
            sls_doc.set_avail_info(text)

            try:
                sls_doc.send_document()
            except Exception as ex:
                _logger.error(ex)


if __name__ == "__main__":
    main()
