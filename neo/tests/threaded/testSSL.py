#
# Copyright (C) 2015-2017  Nexedi SA
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

import unittest
from neo.lib.connection import ClientConnection, ListeningConnection
from neo.lib.protocol import Packets
from .. import Patch, SSL
from . import NEOCluster, test, testReplication


class SSLMixin:

    @classmethod
    def setUpClass(cls):
        NEOCluster.SSL = SSL

    @classmethod
    def tearDownClass(cls):
        NEOCluster.SSL = None


class SSLTests(SSLMixin, test.Test):
    # exclude expected failures
    testStorageDataLock2 = None
    testUndoConflictDuringStore = None

    # With MySQL, this test is expensive.
    # Let's check deduplication of big oids here.
    def testBasicStore(self):
        super(SSLTests, self).testBasicStore(True)

    def testAbortConnection(self, after_handshake=1):
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
            conn.abort()
            fd = connector.getDescriptor()
            while fd in conn.em.reader_set:
                conn.em.poll(1)
            self.assertIs(conn.getConnector(), None)

    def testAbortConnectionBeforeHandshake(self):
        self.testAbortConnection(0)

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

class SSLReplicationTests(SSLMixin, testReplication.ReplicationTests):
    # do not repeat slowest tests with SSL
    testBackupNodeLost = testBackupNormalCase = None


if __name__ == "__main__":
    unittest.main()
