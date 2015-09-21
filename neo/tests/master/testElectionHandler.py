#
# Copyright (C) 2009-2015  Nexedi SA
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
from mock import Mock
from neo.lib import protocol
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, NodeStates
from neo.master.handlers.election import ClientElectionHandler, \
        ServerElectionHandler
from neo.master.app import Application
from neo.lib.exception import ElectionFailure
from neo.lib.connection import ClientConnection

# patch connection so that we can register _addPacket messages
# in mock object
def _addPacket(self, packet):
    if self.connector is not None:
        self.connector._addPacket(packet)

class MasterClientElectionTestBase(NeoUnitTestBase):

    def setUp(self):
        super(MasterClientElectionTestBase, self).setUp()
        self._master_port = 3001

    def identifyToMasterNode(self):
        node = self.app.nm.createMaster(uuid=self.getMasterUUID())
        node.setAddress((self.local_ip, self._master_port))
        self._master_port += 1
        conn = self.getFakeConnection(
                uuid=node.getUUID(),
                address=node.getAddress(),
        )
        node.setConnection(conn)
        return (node, conn)

class MasterClientElectionTests(MasterClientElectionTestBase):

    def setUp(self):
        super(MasterClientElectionTests, self).setUp()
        # create an application object
        config = self.getMasterConfiguration(master_number=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.em = Mock()
        self.app.uuid = self.getMasterUUID()
        self.app.server = (self.local_ip, 10000)
        self.app.name = 'NEOCLUSTER'
        self.election = ClientElectionHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        # apply monkey patches
        ClientConnection._addPacket = _addPacket

    def _tearDown(self, success):
        # restore patched methods
        del ClientConnection._addPacket
        NeoUnitTestBase._tearDown(self, success)

    def _checkUnconnected(self, node):
        addr = node.getAddress()
        self.assertFalse(addr in self.app.negotiating_master_node_set)

    def test_connectionFailed(self):
        node, conn = self.identifyToMasterNode()
        self.assertTrue(node.isUnknown())
        self._checkUnconnected(node)
        self.election.connectionFailed(conn)
        self._checkUnconnected(node)
        self.assertTrue(node.isUnknown())

    def test_connectionCompleted(self):
        node, conn = self.identifyToMasterNode()
        self.assertTrue(node.isUnknown())
        self._checkUnconnected(node)
        self.election.connectionCompleted(conn)
        self._checkUnconnected(node)
        self.assertTrue(node.isUnknown())
        self.checkRequestIdentification(conn)

    def _setNegociating(self, node):
        self._checkUnconnected(node)
        addr = node.getAddress()
        self.app.negotiating_master_node_set.add(addr)

    def test_connectionClosed(self):
        node, conn = self.identifyToMasterNode()
        self._setNegociating(node)
        self.election.connectionClosed(conn)
        self.assertTrue(node.isUnknown())
        addr = node.getAddress()
        self.assertFalse(addr in self.app.negotiating_master_node_set)

    def test_acceptIdentification1(self):
        """ A non-master node accept identification """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), 0, 10, self.app.uuid, None,
            self._getMasterList())
        self.election.acceptIdentification(conn,
            NodeTypes.CLIENT, *args)
        self.assertFalse(node in self.app.negotiating_master_node_set)
        self.checkClosed(conn)

    def test_acceptIdentificationDoesNotKnowPrimary(self):
        master1, master1_conn = self.identifyToMasterNode()
        master1_uuid = master1.getUUID()
        self.election.acceptIdentification(
            master1_conn,
            NodeTypes.MASTER,
            master1_uuid,
            1,
            0,
            self.app.uuid,
            None,
            [(master1.getAddress(), master1_uuid)],
        )
        self.assertEqual(self.app.primary_master_node, None)

    def test_acceptIdentificationKnowsPrimary(self):
        master1, master1_conn = self.identifyToMasterNode()
        master1_uuid = master1.getUUID()
        primary1 = master1.getAddress()
        self.election.acceptIdentification(
            master1_conn,
            NodeTypes.MASTER,
            master1_uuid,
            1,
            0,
            self.app.uuid,
            primary1,
            [(master1.getAddress(), master1_uuid)],
        )
        self.assertNotEqual(self.app.primary_master_node, None)

    def test_acceptIdentificationMultiplePrimaries(self):
        master1, master1_conn = self.identifyToMasterNode()
        master2, master2_conn = self.identifyToMasterNode()
        master3, _ = self.identifyToMasterNode()
        master1_uuid = master1.getUUID()
        master2_uuid = master2.getUUID()
        master3_uuid = master3.getUUID()
        primary1 = master1.getAddress()
        primary3 = master3.getAddress()
        master1_address = master1.getAddress()
        master2_address = master2.getAddress()
        master3_address = master3.getAddress()
        self.election.acceptIdentification(
            master1_conn,
            NodeTypes.MASTER,
            master1_uuid,
            1,
            0,
            self.app.uuid,
            primary1,
            [(master1_address, master1_uuid)],
        )
        self.assertRaises(ElectionFailure, self.election.acceptIdentification,
            master2_conn,
            NodeTypes.MASTER,
            master2_uuid,
            1,
            0,
            self.app.uuid,
            primary3,
            [
                (master1_address, master1_uuid),
                (master2_address, master2_uuid),
                (master3_address, master3_uuid),
            ],
        )

    def test_acceptIdentification3(self):
        """ Identification accepted """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), 0, 10, self.app.uuid, None,
            self._getMasterList())
        self.election.acceptIdentification(conn, NodeTypes.MASTER, *args)
        self.checkUUIDSet(conn, node.getUUID())
        self.assertEqual(self.app.primary is False,
                         self.app.server < node.getAddress())
        self.assertFalse(node in self.app.negotiating_master_node_set)

    def _getMasterList(self, with_node=None):
        master_list = self.app.nm.getMasterList()
        return [(x.getAddress(), x.getUUID()) for x in master_list]


class MasterServerElectionTests(MasterClientElectionTestBase):

    def setUp(self):
        super(MasterServerElectionTests, self).setUp()
        # create an application object
        config = self.getMasterConfiguration(master_number=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.name = 'NEOCLUSTER'
        self.app.em = Mock()
        self.election = ServerElectionHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        for node in self.app.nm.getMasterList():
            node.setState(NodeStates.RUNNING)
        # define some variable to simulate client and storage node
        self.client_address = (self.local_ip, 1000)
        self.storage_address = (self.local_ip, 2000)
        self.master_address = (self.local_ip, 3000)
        # apply monkey patches
        ClientConnection._addPacket = _addPacket

    def _tearDown(self, success):
        NeoUnitTestBase._tearDown(self, success)
        # restore environnement
        del ClientConnection._addPacket

    def test_requestIdentification1(self):
        """ A non-master node request identification """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), node.getAddress(), self.app.name)
        self.assertRaises(protocol.NotReadyError,
            self.election.requestIdentification,
            conn, NodeTypes.CLIENT, *args)

    def test_requestIdentification3(self):
        """ A broken master node request identification """
        node, conn = self.identifyToMasterNode()
        node.setBroken()
        args = (node.getUUID(), node.getAddress(), self.app.name)
        self.assertRaises(protocol.BrokenNodeDisallowedError,
            self.election.requestIdentification,
            conn, NodeTypes.MASTER, *args)

    def test_requestIdentification4(self):
        """ No conflict """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), node.getAddress(), self.app.name)
        self.election.requestIdentification(conn,
            NodeTypes.MASTER, *args)
        self.checkUUIDSet(conn, node.getUUID())
        args = self.checkAcceptIdentification(conn, decode=True)
        (node_type, uuid, partitions, replicas, new_uuid, primary_uuid,
            master_list) = args
        self.assertEqual(node.getUUID(), new_uuid)
        self.assertNotEqual(node.getUUID(), uuid)

    def _getNodeList(self):
        return [x.asTuple() for x in self.app.nm.getList()]

    def __getClient(self):
        uuid = self.getClientUUID()
        conn = self.getFakeConnection(uuid=uuid, address=self.client_address)
        self.app.nm.createClient(uuid=uuid, address=self.client_address)
        return conn

    def __getMaster(self, port=1000, register=True):
        uuid = self.getMasterUUID()
        address = ('127.0.0.1', port)
        conn = self.getFakeConnection(uuid=uuid, address=address)
        if register:
            self.app.nm.createMaster(uuid=uuid, address=address)
        return conn

    def testRequestIdentification1(self):
        """ Check with a non-master node, must be refused """
        conn = self.__getClient()
        self.checkNotReadyErrorRaised(
            self.election.requestIdentification,
            conn=conn,
            node_type=NodeTypes.CLIENT,
            uuid=conn.getUUID(),
            address=conn.getAddress(),
            name=self.app.name
        )

    def _requestIdentification(self):
        conn = self.getFakeConnection()
        peer_uuid = self.getMasterUUID()
        address = (self.local_ip, 2001)
        self.election.requestIdentification(
            conn,
            NodeTypes.MASTER,
            peer_uuid,
            address,
            self.app.name,
        )
        node_type, uuid, partitions, replicas, _peer_uuid, primary, \
            master_list = self.checkAcceptIdentification(conn, decode=True)
        self.assertEqual(node_type, NodeTypes.MASTER)
        self.assertEqual(uuid, self.app.uuid)
        self.assertEqual(partitions, self.app.pt.getPartitions())
        self.assertEqual(replicas, self.app.pt.getReplicas())
        self.assertTrue(address in [x[0] for x in master_list])
        self.assertTrue(self.app.server in [x[0] for x in master_list])
        self.assertEqual(peer_uuid, _peer_uuid)
        return primary

    def testRequestIdentificationDoesNotKnowPrimary(self):
        self.app.primary = False
        self.app.primary_master_node = None
        self.assertEqual(self._requestIdentification(), None)

    def testRequestIdentificationKnowsPrimary(self):
        self.app.primary = False
        primary = (self.local_ip, 3000)
        self.app.primary_master_node = Mock({
            'getAddress': primary,
        })
        self.assertEqual(self._requestIdentification(), primary)

    def testRequestIdentificationIsPrimary(self):
        self.app.primary = True
        primary = self.app.server
        self.app.primary_master_node = Mock({
            'getAddress': primary,
        })
        self.assertEqual(self._requestIdentification(), primary)

    def test_reelectPrimary(self):
        node, conn = self.identifyToMasterNode()
        self.assertRaises(ElectionFailure, self.election.reelectPrimary, conn)


if __name__ == '__main__':
    unittest.main()

