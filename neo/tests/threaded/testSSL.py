#
# Copyright (C) 2015-2019  Nexedi SA
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

import os
import six.moves._thread as _thread
from neo.lib.connection import ClientConnection, ListeningConnection
from neo.lib.connector import SocketConnector
from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets
from .. import getTempDirectory, Patch, SSL, SSL2
from . import BIND, MasterApplication, NEOCluster, \
    test, testPack, testReplication
from .testPack import GCTests, ReflinkCluster


class SSLMixin(object):

    @classmethod
    def setUpClass(cls):
        super(SSLMixin, cls).setUpClass()
        NEOCluster.SSL = SSL
        ReflinkCluster.SSL = SSL2

    @classmethod
    def tearDownClass(cls):
        NEOCluster.SSL = None
        del ReflinkCluster.SSL
        super(SSLMixin, cls).tearDownClass()


class SSLTests(SSLMixin, test.Test):
    # exclude expected failures
    testStorageDataLock2 = None
    testUndoConflictDuringStore = None

    # With MySQL, this test is expensive.
    # Let's check deduplication of big oids here.
    def testBasicStore(self):
        super(SSLTests, self).testBasicStore(True)

    def testAbortConnection(self, after_handshake=1, abortf=None):
        with self.getLoopbackConnection() as conn:
            conn.ask(Packets.Ping())
            connector = conn.getConnector()
            del connector.connect_limit[connector.addr]
            conn.em.poll(1)
            self.assertTrue(isinstance(connector,
                connector.SSLHandshakeConnectorClass))
            self.assertNotIn(connector.getDescriptor(), conn.em.writer_set)
            if after_handshake:
                while not isinstance(connector, connector.SSLConnectorClass):
                    conn.em.poll(1)
            if abortf is None:
                abortf = conn.__class__.abort
            abortf(conn)
            fd = connector.getDescriptor()
            while fd in conn.em.reader_set:
                conn.em.poll(1)
            self.assertIs(conn.getConnector(), None)

    def testAbortConnectionBeforeHandshake(self):
        self.testAbortConnection(0)

    def testReceiveVSEOF(self):
        # verify that _SSL.receive correctly handles non-ragged EOF.
        # ( it used to hang in infinite busy loop because _SSL was not checking
        #   return from socket.recv for b'' )
        def _(conn):
            conn.em.poll(0)
            conn.connector.shutdown()
        self.testAbortConnection(abortf=_)

    def testSSLVsNoSSL(self):
        def __init__(orig, self, app, *args, **kw):
            with Patch(app, ssl=None):
                orig(self, app, *args, **kw)
        for cls in (ListeningConnection, # SSL connecting to non-SSL
                    ClientConnection,    # non-SSL connecting to SSL
                    ):
            with Patch(cls, __init__=__init__), \
                 self.getLoopbackConnection() as conn:
                while not conn.isClosed():
                    conn.em.poll(1)

    def testReloadSSL(self):
        socket_prefix = '\0neo.tests[%s]:' % os.getpid()
        def makeApp(name):
            name = socket_prefix + name
            reload_sock.append(name)
            app.append(MasterApplication(replicas=0, partitions=1,
                address=BIND, ssl_credentials=ssl + (name,)))
            return app[-1]
        class Handler(EventHandler):
            completed = failed = 0
            def connectionCompleted(self, conn):
                self.completed += 1
            def connectionFailed(self, conn):
                self.failed += 1
        def check(expect_failed):
            conn = ClientConnection(c, handler, node)
            try:
                completed = handler.completed
                failed = handler.failed
                while True:
                    app[1].em.poll(1)
                    if (completed != handler.completed
                        or failed != handler.failed):
                        break
            finally:
                conn.close()
            self.assertEqual(handler.completed, completed + 1 - expect_failed)
            self.assertEqual(handler.failed, failed + expect_failed)
        tmp = os.path.join(getTempDirectory(), self.id())
        os.mkdir(tmp)
        ssl_selector = self.sslSelector(tmp)
        ssl = next(ssl_selector)
        reload_sock = []
        app = []
        try:
            s = makeApp('server')
            s.listening_conn = ListeningConnection(s, EventHandler(s), s.server)
            @self.newThread
            def t():
                while True:
                    s.em.poll(1)
            with Patch(SocketConnector, CONNECT_LIMIT=0):
                try:
                    c = makeApp('client')
                    handler = Handler(c)
                    node = c.nm.createMaster(
                        address=s.listening_conn.getAddress(), uuid=s.uuid)
                    check(0)
                    next(ssl_selector)
                    check(0)
                    self.sslReload(reload_sock[0])
                    check(1)
                    t2 = self.newThread(self.sslReload, reload_sock[1])
                    c.em.poll(1)
                    t2.join()
                    check(0)
                finally:
                    s.em.wakeup(_thread.exit)
                    t.join()
        finally:
            for x in app:
                x.close()


class SSLReplicationTests(SSLMixin, testReplication.ReplicationTests):
    # do not repeat slowest tests with SSL
    testBackupNodeLost = testBackupNormalCase = None

class SSLGCTests(SSLMixin, GCTests):
    pass
