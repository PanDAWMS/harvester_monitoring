import os

from config import Config
from sqlite_cache import Sqlite
from pandadb import PandaDB
from es import Es
from notifications import Notifications
from logger import ServiceLogger
from datetime import datetime
from cernservicexml import ServiceDocument, XSLSPublisher

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_logger = ServiceLogger("schedd_monitoring").logger

def main():

    config = Config(BASE_DIR + '/schedd_configuration/', type='schedd')
    sqlite = Sqlite(BASE_DIR + '/storage/hsm.db', config.XMLconfiguration)
    es = Es(BASE_DIR + '/settings.ini')

    schedd_metrics = es.get_schedd_metrics()
    sqlite.scheddhosts_availability(schedd_metrics)
    #
    submissionhosts = sqlite.get_data(type='schedd')

    for host in submissionhosts:
        if host != 'none':
            availability = submissionhosts[host]['availability']
            notificated = submissionhosts[host]['notificated']
            contacts = submissionhosts[host]['contacts']
            text = submissionhosts[host]['errorsdesc']
            errors = sqlite.get_schedd_history_logs(submissionhost=host)
            #pravailability = instances[instance][harvesterhost]['pravailability']
            if (availability == 0 or availability == 10 or availability == 50):
                if len(errors) > 0:
                    mailtext = ''
                    for error in errors:
                        if error['notificated'] == 0:
                            mailtext = mailtext+error['fulltext']
                            sqlite.update_schedd_entry('SCHEDDHISTORYLOG', 'notificated', 1,  host, checkmetrictime=error['checkmetrictime'])
                            sqlite.update_schedd_entry('SCHEDDHISTORYLOG', 'notificationtime', str(datetime.utcnow()), host,
                                                checkmetrictime=error['checkmetrictime'])
                    if mailtext !='':
                        email = Notifications(text=mailtext,
                                        subject='Service issues on submissionhost: {0}'.format(host),
                                        to=contacts)
                        email.send_notification_email()
                        sqlite.update_schedd_entry('SUBMISSIONHOSTS', 'notificated', 1, host)
                        email = {}
            elif availability == 100 and notificated == 1:
                sqlite.update_schedd_entry('SUBMISSIONHOSTS', 'notificated', 0, host)
            doc = ServiceDocument('schedd_{0}'.format(host), availability=availability, contact=','.join(contacts), availabilitydesc="Submissionhost:{0}".format(host), availabilityinfo="{0}".format(text))
            try:
                XSLSPublisher.send(doc)
                _logger.debug(str(doc.__dict__))
            except Exception as ex:
                _logger.error(ex.message)
                print ex.message
            doc = {}
if __name__ == "__main__":
    main()