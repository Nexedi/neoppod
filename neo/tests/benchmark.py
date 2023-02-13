from __future__ import print_function
import argparse
import sys
import smtplib
import platform
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

MAIL_SERVER = '127.0.0.1:25'

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
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter)
        # register common options
        _ = parser.add_argument
        _('--title')
        _('--mail-to', action='append')
        _('--mail-from')
        _('--mail-server')
        self.add_options(parser)
        # check common arguments
        args = parser.parse_args()
        if bool(args.mail_to) ^ bool(args.mail_from):
            sys.exit('Need a sender and recipients to mail report')
        mail_server = args.mail_server or MAIL_SERVER
        # check specifics arguments
        self._config = AttributeDict()
        self._config.update(self.load_options(args))
        self._config.update(
            title = args.title or self.__class__.__name__,
            mail_from = args.mail_from,
            mail_to = args.mail_to,
            mail_server = mail_server.split(':'),
        )

    def add_status(self, key, value):
        self._status.append((key, value))

    def build_report(self, content):
        fmt = "%-25s : %s"
        py_impl = platform.python_implementation()
        if py_impl == 'PyPy':
            info = sys.pypy_version_info
            py_impl += ' %s.%s.%s' % info[:3]
            kind = info.releaselevel
            if kind != 'final':
                py_impl += kind[0] + str(info.serial)
        status = "\n".join([fmt % item for item in [
            ('Title', self._config.title),
            ('Date', datetime.date.today().isoformat()),
            ('Node', platform.node()),
            ('Machine', platform.machine()),
            ('System', platform.system()),
            ('Python', '%s [%s]' % (platform.python_version(), py_impl)),
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
                print("Mail for %s fails" % recipient)
        s.close()

    def run(self):
        subject, report = self.start()
        report = self.build_report(report)
        if self._config.mail_to:
            self.send_report(subject, report)
        print(subject)
        if report:
            print()
            print(report, end='')

    def was_successful(self):
        return self._successful

    def add_options(self, parser):
        """ Append options to command line parser """
        raise NotImplementedError

    def load_options(self, args):
        """ Check options and return a configuration dict """
        raise NotImplementedError

    def start(self):
        """ Run the test """
        raise NotImplementedError

