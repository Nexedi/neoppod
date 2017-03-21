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
from neo.lib.protocol import Packets
from .. import SSL
from . import NEOCluster, with_cluster, test, testReplication


class SSLMixin:

    @classmethod
    def setUpClass(cls):
        NEOCluster.SSL = SSL

    @classmethod
    def tearDownClass(cls):
        NEOCluster.SSL = None


class SSLTests(SSLMixin, test.Test):
    # exclude expected failures
    testDeadlockAvoidance = None
    testUndoConflict = testUndoConflictDuringStore = None

    if 1:
        testShutdownWithSeveralMasterNodes = unittest.skip("fails randomly")(
            test.Test.testShutdown.__func__)

        @with_cluster(partitions=10, replicas=1, storage_count=3)
        def testShutdown(self, cluster):
            self._testShutdown(cluster)

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

class SSLReplicationTests(SSLMixin, testReplication.ReplicationTests):
    # do not repeat slowest tests with SSL
    testBackupNodeLost = testBackupNormalCase = None


if __name__ == "__main__":
    unittest.main()
