import os
import logging

from config import Config
from sqlite_cache import Sqlite
from pandadb import PandaDB
from es import Es
from notifications import Notifications
from logger import ServiceLogger
from datetime import datetime
from cernservicexml import ServiceDocument, XSLSPublisher

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_logger = ServiceLogger("main").logger

def main():

    config = Config(BASE_DIR + '/configuration/')
    sqlite = Sqlite(BASE_DIR + '/storage/hsm.db', config.XMLconfiguration)
    pandadb = PandaDB(BASE_DIR + '/settings.ini')
    es = Es(BASE_DIR + '/settings.ini')

    metrics = pandadb.get_db_metrics()
    dictHarvesterHosts, dictHarvesterInstnaces = es.get_last_submittedtime()
    sqlite.instances_availability(dictHarvesterInstnaces, metrics)

    instances = sqlite.get_data()

    for instance in instances:
        for harvesterhost in instances[instance]:
            if harvesterhost != 'none':
                availability = instances[instance][harvesterhost]['availability']
                notificated = instances[instance][harvesterhost]['notificated']
                contacts = instances[instance][harvesterhost]['contacts']
                text = instances[instance][harvesterhost]['errorsdesc']
                errors = sqlite.get_history_logs(harvesterid=instance, harvesterhost=harvesterhost)
                #pravailability = instances[instance][harvesterhost]['pravailability']
                if (availability == 0 or availability == 10 or availability == 50):
                    if len(errors) > 0:
                        mailtext = ''
                        for error in errors:
                            if error['notificated'] == 0:
                                mailtext = mailtext+error['fulltext']
                                sqlite.update_entry('HISTORYLOG', 'notificated', 1, instance, harvesterhost, checkmetrictime=error['checkmetrictime'])
                                sqlite.update_entry('HISTORYLOG', 'notificationtime', str(datetime.utcnow()), instance, harvesterhost,
                                                    checkmetrictime=error['checkmetrictime'])
                        if mailtext !='':
                            email = Notifications(text=mailtext,
                                            subject='Service issues on {0} {1}'.format(instance, harvesterhost),
                                            to=contacts)
                            #email.send_notification_email()
                            sqlite.update_entry('INSTANCES', 'notificated', 1, instance, harvesterhost)
                            email = {}
                elif availability == 100 and notificated == 1:
                    sqlite.update_entry('INSTANCES', 'notificated', 0, instance, harvesterhost)
                host = harvesterhost.split('.')[0]
                doc = ServiceDocument('harv_{0}_{1}'.format(instance, host), availability=availability, contact=','.join(contacts), availabilitydesc="PandaHarvester instance:{0}".format(instance), availabilityinfo="{0}".format(text))
                try:
                    #XSLSPublisher.send(doc)
                    _logger.debug(str(doc.__dict__))
                except Exception as ex:
                    _logger.error(ex.message)
                    print ex.message
                doc = {}

if __name__ == "__main__":
    main()