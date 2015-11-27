#
# Copyright (C) 2015  Nexedi SA
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
from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets
from .. import SSL
from . import MasterApplication, NEOCluster, test, testReplication


class SSLMixin:

    @classmethod
    def setUpClass(cls):
        NEOCluster.SSL = SSL

    @classmethod
    def tearDownClass(cls):
        NEOCluster.SSL = None


class SSLTests(SSLMixin, test.Test):
    # exclude expected failures
    testDeadlockAvoidance = testStorageFailureDuringTpcFinish = None

    def testAbortConnection(self):
        app = MasterApplication(getSSL=SSL, getReplicas=0, getPartitions=1)
        handler = EventHandler(app)
        app.listening_conn = ListeningConnection(app, handler, app.server)
        node = app.nm.createMaster(address=app.listening_conn.getAddress(),
                                   uuid=app.uuid)
        for after_handshake in 1, 0:
            conn = ClientConnection(app, handler, node)
            conn.ask(Packets.Ping())
            connector = conn.getConnector()
            del connector.connect_limit[connector.addr]
            app.em.poll(1)
            self.assertTrue(isinstance(connector,
                connector.SSLHandshakeConnectorClass))
            self.assertNotIn(connector.getDescriptor(), app.em.writer_set)
            if after_handshake:
                while not isinstance(connector, connector.SSLConnectorClass):
                    app.em.poll(1)
            conn.abort()
            fd = connector.getDescriptor()
            while fd in app.em.reader_set:
                app.em.poll(1)
            self.assertIs(conn.getConnector(), None)

class SSLReplicationTests(SSLMixin, testReplication.ReplicationTests):
    # do not repeat slowest tests with SSL
    testBackupNodeLost = testBackupNormalCase = None


if __name__ == "__main__":
    unittest.main()
