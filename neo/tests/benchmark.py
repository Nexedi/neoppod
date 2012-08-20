
import sys
import smtplib
import optparse
import platform
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

MAIL_SERVER = '127.0.0.1:25'

from neo.lib import logging
logging.backlog()

class AttributeDict(dict):

    def __getattr__(self, item):
        return self.__getitem__(item)

class BenchmarkRunner(object):
    """
        Base class for a command-line benchmark test runner.
    """

    def __init__(self):
        self._successful = True
        self._status = []
        parser = optparse.OptionParser()
        # register common options
        parser.add_option('', '--title')
        parser.add_option('', '--mail-to', action='append')
        parser.add_option('', '--mail-from')
        parser.add_option('', '--mail-server')
        parser.add_option('', '--repeat', type='int', default=1)
        self.add_options(parser)
        # check common arguments
        options, self._args = parser.parse_args()
        if bool(options.mail_to) ^ bool(options.mail_from):
            sys.exit('Need a sender and recipients to mail report')
        mail_server = options.mail_server or MAIL_SERVER
        # check specifics arguments
        self._config = AttributeDict()
        self._config.update(self.load_options(options, self._args))
        self._config.update(
            title = options.title or self.__class__.__name__,
            mail_from = options.mail_from,
            mail_to = options.mail_to,
            mail_server = mail_server.split(':'),
            repeat = options.repeat,
        )

    def add_status(self, key, value):
        self._status.append((key, value))

    def build_report(self, content):
        fmt = "%-25s : %s"
        status = "\n".join([fmt % item for item in [
            ('Title', self._config.title),
            ('Date', datetime.date.today().isoformat()),
            ('Node', platform.node()),
            ('Machine', platform.machine()),
            ('System', platform.system()),
            ('Python', platform.python_version()),
        ]])
        status += '\n\n'
        status += "\n".join([fmt % item for item in self._status])
        return "%s\n\n%s" % (status, content)

    def send_report(self, subject, report):
        # build report
        # build email
        msg = MIMEMultipart()
        msg['Subject'] = '%s: %s' % (self._config.title, subject)
        msg['From'] = self._config.mail_from
        msg['To'] = ', '.join(self._config.mail_to)
        msg['X-ERP5-Tests'] = 'NEO'
        if self._successful:
            msg['X-ERP5-Tests-Status'] = 'OK'
        msg.epilogue = ''
        msg.attach(MIMEText(report))

        # send it
        s = smtplib.SMTP()
        s.connect(*self._config.mail_server)
        mail = msg.as_string()
        for recipient in self._config.mail_to:
            try:
                s.sendmail(self._config.mail_from, recipient, mail)
            except smtplib.SMTPRecipientsRefused:
                print "Mail for %s fails" % recipient
        s.close()

    def run(self):
        subject, report = self.start()
        report = self.build_report(report)
        if self._config.mail_to:
            self.send_report(subject, report)
        print subject
        print
        print report

    def was_successful(self):
        return self._successful

    def add_options(self, parser):
        """ Append options to command line parser """
        raise NotImplementedError

    def load_options(self, options, args):
        """ Check options and return a configuration dict """
        raise NotImplementedError

    def start(self):
        """ Run the test """
        raise NotImplementedError

