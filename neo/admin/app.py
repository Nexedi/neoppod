# -*- coding: utf-8 -*-
#
# Copyright (C) 2006-2019  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import getpass, os, smtplib
from collections import Counter
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate
from time import time
from traceback import format_exc
from neo.lib import logging
from neo.lib.app import BaseApplication, buildOptionParser
from neo.lib.connection import ClientConnection, ListeningConnection, \
    ConnectionClosed
from neo.lib.exception import PrimaryFailure
from .handler import AdminEventHandler, BackupHandler, MasterEventHandler, \
    UpstreamAdminHandler, NOT_CONNECTED_MESSAGE
from neo.lib.bootstrap import BootstrapManager
from neo.lib.logger import INF
from neo.lib.protocol import \
    CellStates, ClusterStates, Errors, NodeTypes, Packets
from neo.lib.debug import register as registerLiveDebugger
from neo.lib.util import add64, datetimeFromTID, dump

class Monitor(object):

    def __init__(self):
        self.down = 0
        self.monitor_changed = False
        self.pt_summary = None

    def askLastIds(self, conn,
            _askLastTransaction=Packets.AskLastTransaction(),
            _askRecovery=Packets.AskRecovery()):
        if self.cluster_state == ClusterStates.BACKINGUP:
            conn.ask(_askRecovery)
        conn.ask(_askLastTransaction)

    @property
    def operational(self):
        return self.cluster_state in (ClusterStates.BACKINGUP,
                                      ClusterStates.RUNNING)

    @property
    def severity(self):
        return (2 if self.down or not self.operational else
                1 if list(self.pt_summary) != [CellStates.UP_TO_DATE] or
                     isinstance(self, Backup) and
                     self.cluster_state != ClusterStates.BACKINGUP else
                0)

    def formatSummary(self, upstream=None):
        summary = self.pt_summary
        summary = '%s; %s' % (self.cluster_state,
            ', '.join('%s=%s' % pt for pt in sorted(summary.iteritems()))
            ) if summary else str(self.cluster_state)
        if self.down:
            summary += '; DOWN=%s' % self.down
        if self.operational:
            backup = self.cluster_state == ClusterStates.BACKINGUP
            tid = self.backup_tid if backup else self.ltid
            x = datetimeFromTID(tid)
            if upstream and backup:
                lag = (upstream[0] - x).total_seconds()
                if lag or tid >= upstream[1]:
                    lagging = self.max_lag < lag
                else:
                    lag = 'ε'
                    lagging = self.max_lag <= 0
                extra = '; lag=%s' % lag
                if self.lagging != lagging:
                    self.lagging = lagging
                    self.monitor_changed = True
            else:
                extra = ' (%s)' % x
            return (x, tid), '%s; ltid=%s%s' % (summary, dump(tid), extra)
        return None, summary

class Backup(Monitor):

    cluster_state = None
    conn = None
    lagging = False
    max_lag = 0

@buildOptionParser
class Application(BaseApplication, Monitor):
    """The storage node application."""

    @classmethod
    def _buildOptionParser(cls):
        _ = cls.option_parser
        _.description = "NEO Admin node"
        cls.addCommonServerOptions('admin', '127.0.0.1:9999')

        hint = ' (the option can be repeated)'
        _ = _.group('admin')
        _('monitor-email', multiple=True,
            help='recipient email for notifications' + hint)
        _('monitor-backup', multiple=True,
            help='name of backup cluster to monitor' + hint)
        _('smtp', metavar='HOST[:PORT]',
            help='SMTP for email notifications')
        _.int('i', 'nid',
            help="specify an NID to use for this process (testing purpose)")

    def __init__(self, config):
        BaseApplication.__init__(self,
            config.get('ssl'), config.get('dynamic_master_list'))
        for address in config['masters']:
            self.nm.createMaster(address=address)

        self.name = config['cluster']
        self.server = config['bind']

        self.backup_dict = {x: Backup()
            for x in config.get('monitor_backup', ())}
        self.email_list = config.get('monitor_email', ())
        if self.email_list:
            self.smtp = smtplib.SMTP()
            self.smtp_host = config.get('smtp') or 'localhost'
            email_from = os.getenv('EMAIL')
            if not email_from:
              try:
                email_from = getpass.getuser()
              except Exception:
                email_from = None
            self.email_from = formataddr(("NEO " + self.name, email_from))
        self.smtp_exc = None
        self.smtp_retry = INF
        self.notifying = set()

        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = config.get('nid')
        logging.node(self.name, self.uuid)
        self.backup_handler = BackupHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.upstream_admin_handler = UpstreamAdminHandler(self)
        self.cluster_state = None
        self.upstream_admin = self.upstream_admin_conn = None
        self.reset()
        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        super(Application, self).close()

    def reset(self):
        Monitor.__init__(self)
        self.asking_monitor_information = []
        self.master_conn = None
        self.master_node = None

    def log(self):
        self.em.log()
        self.nm.log()
        if self.pt is not None:
            self.pt.log()

    def run(self):
        try:
            self._run()
        except Exception:
            logging.exception('Pre-mortem data:')
            self.log()
            logging.flush()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port.
        handler = AdminEventHandler(self)
        self.listening_conn = ListeningConnection(self, handler, self.server)

        while self.cluster_state != ClusterStates.STOPPING:
            self.connectToPrimary()
            try:
                while True:
                    self.em.poll(1)
            except PrimaryFailure:
                logging.error('primary master is down')
        self.listening_conn.close()
        while not self.em.isIdle():
            self.em.poll(1)

    def connectToPrimary(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage.
        """
        self.cluster_state = None
        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, NodeTypes.ADMIN, self.server,
                                     backup=list(self.backup_dict))
        self.master_node, self.master_conn = bootstrap.getPrimaryConnection()

        # passive handler
        self.master_conn.setHandler(self.master_event_handler)
        self.master_conn.ask(Packets.AskClusterState())

    def connectToUpstreamAdmin(self):
        if self.listening_conn: # if running
            conn = self.upstream_admin_conn = ClientConnection(
                self, self.upstream_admin_handler, self.upstream_admin)
            conn.ask(Packets.RequestIdentification(NodeTypes.ADMIN,
                None, None, self.name, None, {}))

    def partitionTableUpdated(self):
        pt = self.pt
        if pt:
            down_set = set()
            pt_summary = Counter()
            for offset in xrange(pt.np):
                for cell in pt.getCellList(offset):
                    node = cell.getNode()
                    if not node.isRunning():
                        down_set.add(node)
                    pt_summary.update((cell.getState(),))
            self.updateMonitorInformation(None,
                down=len(down_set), pt_summary=dict(pt_summary))

    def askMonitorInformation(self, conn):
        asking = self.asking_monitor_information or self.notifying
        self.asking_monitor_information.append((conn, conn.getPeerId()))
        if not asking:
            self._notify(self.operational)

    def updateMonitorInformation(self, name, **kw):
        monitor = self if name is None else self.backup_dict[name]
        kw = {k: v for k, v in kw.iteritems() if v != getattr(monitor, k)}
        if not kw:
            return
        monitor.monitor_changed = True
        monitor.__dict__.update(kw)
        if name is None and self.upstream_admin_conn:
            self.upstream_admin_conn.send(Packets.NotifyMonitorInformation(kw))
        if not self.notifying:
            self.em.setTimeout(None, None)
            self._notify(self.operational)

    def _notify(self, ask_ids=True,
                _askLastTransaction=Packets.AskLastTransaction(),
                _askRecovery=Packets.AskRecovery()):
        if ask_ids:
            self.askLastIds(self.master_conn)
            self.notifying = notifying = {None}
            for name, monitor in self.backup_dict.iteritems():
                if monitor.operational:
                    monitor.askLastIds(monitor.conn)
                    notifying.add(name)
        if self.notifying or self.cluster_state is None is not self.master_conn:
            return
        severity = [], [], []
        my_severity = self.severity
        severity[my_severity].append(self.name)
        changed = set()
        if self.monitor_changed:
            self.monitor_changed = False
            changed.add(self.name)
        if self.master_conn is None:
            body = NOT_CONNECTED_MESSAGE
        else:
            upstream, body = self.formatSummary()
            body = [body]
            for name, backup in self.backup_dict.iteritems():
                body += '', name, '    ' + backup.formatSummary(upstream)[1]
                severity[backup.severity or backup.lagging].append(name)
                if backup.monitor_changed:
                    backup.monitor_changed = False
                    changed.add(name)
            body = '\n'.join(body)
        if changed or self.smtp_retry < time():
            logging.debug('monitor notification')
            email_list = self.email_list
            while email_list: # not a loop
                msg = MIMEText(body + (self.smtp_exc or ''))
                msg['Date'] = formatdate()
                clusters, x = severity[1:]
                while 1:
                    if x:
                        clusters = clusters + x
                        x = 'PROBLEM'
                    elif clusters:
                        x = 'WARNING'
                    else:
                        x = 'OK'
                        break
                    clusters = changed.intersection(clusters)
                    if clusters:
                        x += ' (%s)' % ', '.join(sorted(clusters))
                    break
                msg['Subject'] = 'NEO monitoring: ' + x
                msg['From'] = self.email_from
                msg['To'] = ', '.join(email_list)
                s = self.smtp
                try:
                    s.connect(self.smtp_host)
                    s.sendmail(None, email_list, msg.as_string())
                except Exception:
                    x = format_exc()
                    logging.error(x)
                    if changed or not self.smtp_exc:
                        self.smtp_exc = (
                            "\n\nA notification could not be sent at %s:\n\n%s"
                            % (msg['Date'], x))
                    retry = self.smtp_retry = time() + 600
                else:
                    self.smtp_exc = None
                    self.smtp_retry = INF
                    if not (self.operational and any(monitor.operational
                            for monitor in self.backup_dict.itervalues())):
                        break
                    retry = time() + 600
                finally:
                    s.close()
                self.em.setTimeout(retry, self._notify)
                break
        neoctl = self.asking_monitor_information
        if neoctl:
            del severity[my_severity][0]
            if self.smtp_exc:
                my_severity = 2
                body += self.smtp_exc
            severity[1].sort()
            severity[2].sort()
            severity[my_severity].insert(0, None)
            p = Packets.AnswerMonitorInformation(severity[1], severity[2], body)
            for conn, msg_id in neoctl:
                try:
                    conn.send(p, msg_id)
                except ConnectionClosed:
                    pass
            del self.asking_monitor_information[:]

    def maybeNotify(self, name):
        try:
            self.notifying.remove(name)
        except KeyError:
            return
        self._notify(False)

    def sendPartitionTable(self, conn, min_offset, max_offset, uuid):
        pt = self.pt
        if max_offset == 0:
            max_offset = pt.getPartitions()
        try:
            row_list = map(pt.getRow, xrange(min_offset, max_offset))
        except IndexError:
            conn.send(Errors.ProtocolError('invalid partition table offset'))
        else:
            conn.answer(Packets.AnswerPartitionList(
                pt.getID(), pt.getReplicas(), row_list))
