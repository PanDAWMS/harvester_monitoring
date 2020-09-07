import os
import subprocess
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from datetime import datetime

from libs.config import Config
from libs.sqlite_cache import Sqlite
from libs.pandadb import PandaDB
from libs.es import Es
from libs.notifications import Notifications
from libs.kibanaXML import xml_doc

from logger import ServiceLogger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_logger = ServiceLogger("harvester_monitoring", __file__).logger


def main():

    config = Config(BASE_DIR + '/configuration/')
    sqlite = Sqlite(BASE_DIR + '/storage/hsm.db', config.XMLconfiguration)

    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    pandadb = PandaDB(settings)
    es = Es(settings)

    metrics = pandadb.get_db_metrics()

    dictHarvesterInstnaces = es.get_workers_stats()
    sqlite.instances_availability(dictHarvesterInstnaces, metrics)

    instances = sqlite.get_data()



    for instance in instances:
        if instance not in list(config.XMLconfiguration.keys()):
            continue
        for harvesterhost in instances[instance]:
            kibana_xml = xml_doc()

            harvesterhosts_config = list(config.XMLconfiguration[instance].keys())
            if harvesterhost not in harvesterhosts_config:
                continue
            if harvesterhost != 'none':
                availability = instances[instance][harvesterhost]['availability']
                notificated = instances[instance][harvesterhost]['notificated']
                contacts = instances[instance][harvesterhost]['contacts']
                text = instances[instance][harvesterhost]['errorsdesc']
                errors = sqlite.get_history_logs(harvesterid=instance, harvesterhost=harvesterhost)

                if (availability == 0 or availability == 10 or availability == 50):
                    if len(errors) > 0:
                        mailtext = ''
                        for error in errors:
                            if error['notificated'] == 0:
                                mailtext = mailtext + error['fulltext']
                                sqlite.update_entry('HISTORYLOG', 'notificated', 1, instance, harvesterhost,
                                                    checkmetrictime=error['checkmetrictime'])
                                sqlite.update_entry('HISTORYLOG', 'notificationtime', str(datetime.utcnow()), instance,
                                                    harvesterhost,
                                                    checkmetrictime=error['checkmetrictime'])
                        if mailtext != '':
                            email = Notifications(text=mailtext,
                                                  subject='Service issues on {0} {1}'.format(instance, harvesterhost),
                                                  to=contacts)
                            email.send_notification_email()
                            sqlite.update_entry('INSTANCES', 'notificated', 1, instance, harvesterhost)
                            email = {}
                elif availability == 100 and notificated == 1:
                    sqlite.update_entry('INSTANCES', 'notificated', 0, instance, harvesterhost)

                id = 'PandaHarvesterHSM'
                kibana_xml.set_id('%s_%s' % (id, (str(harvesterhost).split('.'))[0]))
                kibana_xml.set_availability(str(availability))
                kibana_xml.set_status(availability)
                kibana_xml.set_avail_desc(instance)
                kibana_xml.set_avail_info(text)

                try:
                    tmp_xml = kibana_xml.print_xml('status')
                    file_name = '%s/xml/%s_%s_kibana.xml' % (BASE_DIR, 'PandaHarvesterHSM', (str(harvesterhost).split('.'))[0])
                    tmp_file = open(file_name, 'w')
                    tmp_file.write(tmp_xml)
                    tmp_file.close()
                    subprocess.call(["curl", "-F", "file=@%s" % file_name, 'xsls.cern.ch'])
                except Exception as ex:
                    print(file_name)
                    _logger.error(ex)
                    print(ex)

if __name__ == "__main__":
    main()
