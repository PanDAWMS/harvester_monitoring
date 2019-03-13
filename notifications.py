import smtplib
from logger import ServiceLogger

_logger = ServiceLogger("notifications").logger

class Notifications:

    def __init__(self, to, mailserver = "localhost", fromemail="noreply@mail.cern.ch",
                                text='', subject=''):
        self.to = to
        self.mailserver = mailserver
        self.fromemail = fromemail
        self.text = text
        self.subject = subject

    def send_notification_email(self):
        """
        Send notification email
        """
        SERVER = self.mailserver
        FROM = self.fromemail
        TO = self.to
        SUBJECT = self.subject
        TEXT = self.text

        message = """\
From: {0}
To: {1}
Subject: {2}
{3}""".format(FROM, ", ".join(TO), SUBJECT, TEXT)

        server = smtplib.SMTP(SERVER)
        server.sendmail(FROM, TO, message)
        server.quit()
        _logger.info("Notification message was sent {0}".format(message))