#
# Copyright (C) 2009-2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import unittest
from mock import Mock
from neo.lib import protocol
from neo.tests import NeoUnitTestBase
from neo.lib.protocol import Packet, NodeTypes, NodeStates
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

class MasterClientElectionTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1)
        self.app = Application(config)
        self.app.pt.clear()
        self.app.em = Mock()
        self.app.uuid = self._makeUUID('M')
        self.app.server = (self.local_ip, 10000)
        self.app.name = 'NEOCLUSTER'
        self.election = ClientElectionHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        for node in self.app.nm.getMasterList():
            self.app.unconnected_master_node_set.add(node.getAddress())
        # define some variable to simulate client and storage node
        self.storage_port = 10021
        self.master_port = 10011
        # apply monkey patches
        self._addPacket = ClientConnection._addPacket
        ClientConnection._addPacket = _addPacket

    def tearDown(self):
        # restore patched methods
        ClientConnection._addPacket = self._addPacket
        NeoUnitTestBase.tearDown(self)

    def identifyToMasterNode(self):
        node = self.app.nm.getMasterList()[0]
        node.setUUID(self.getNewUUID())
        conn = self.getFakeConnection(uuid=node.getUUID(),
                address=node.getAddress())
        return (node, conn)

    def _checkUnconnected(self, node):
        addr = node.getAddress()
        self.assertTrue(addr in self.app.unconnected_master_node_set)
        self.assertFalse(addr in self.app.negotiating_master_node_set)

    def _checkNegociating(self, node):
        addr = node.getAddress()
        self.assertTrue(addr in self.app.negotiating_master_node_set)
        self.assertFalse(addr in self.app.unconnected_master_node_set)

    def test_connectionStarted(self):
        node, conn = self.identifyToMasterNode()
        self.assertTrue(node.isUnknown())
        self._checkUnconnected(node)
        self.election.connectionStarted(conn)
        self.assertTrue(node.isUnknown())
        self._checkNegociating(node)

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
        self.checkAskPrimary(conn)

    def _setNegociating(self, node):
        self._checkUnconnected(node)
        addr = node.getAddress()
        self.app.negotiating_master_node_set.add(addr)
        self.app.unconnected_master_node_set.discard(addr)
        self._checkNegociating(node)

    def test_connectionClosed(self):
        node, conn = self.identifyToMasterNode()
        self._setNegociating(node)
        self.election.connectionClosed(conn)
        self.assertTrue(node.isUnknown())
        addr = node.getAddress()
        self.assertFalse(addr in self.app.unconnected_master_node_set)
        self.assertFalse(addr in self.app.negotiating_master_node_set)

    def test_acceptIdentification1(self):
        """ A non-master node accept identification """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), 0, 10, self.app.uuid)
        self.election.acceptIdentification(conn,
            NodeTypes.CLIENT, *args)
        self.assertFalse(node in self.app.unconnected_master_node_set)
        self.assertFalse(node in self.app.negotiating_master_node_set)
        self.checkClosed(conn)

    def test_acceptIdentification2(self):
        """ UUID conflict """
        node, conn = self.identifyToMasterNode()
        new_uuid = self._makeUUID('M')
        args = (node.getUUID(), 0, 10, new_uuid)
        self.assertRaises(ElectionFailure, self.election.acceptIdentification,
            conn, NodeTypes.MASTER, *args)
        self.assertEqual(self.app.uuid, new_uuid)

    def test_acceptIdentification3(self):
        """ Identification accepted """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), 0, 10, self.app.uuid)
        self.election.acceptIdentification(conn, NodeTypes.MASTER, *args)
        self.checkUUIDSet(conn, node.getUUID())
        self.assertTrue(self.app.primary or node.getUUID() < self.app.uuid)
        self.assertFalse(node in self.app.negotiating_master_node_set)

    def _getMasterList(self, with_node=None):
        master_list = self.app.nm.getMasterList()
        return [(x.getAddress(), x.getUUID()) for x in master_list]

    def test_answerPrimary1(self):
        """ Multiple primary masters -> election failure raised """
        node, conn = self.identifyToMasterNode()
        self.app.primary = True
        self.app.primary_master_node = node
        master_list = self._getMasterList()
        self.assertRaises(ElectionFailure, self.election.answerPrimary,
                conn, self.app.uuid, master_list)

    def test_answerPrimary2(self):
        """ Don't known who's the primary """
        node, conn = self.identifyToMasterNode()
        master_list = self._getMasterList()
        self.election.answerPrimary(conn, None, master_list)
        self.assertFalse(self.app.primary)
        self.assertEqual(self.app.primary_master_node, None)
        self.checkRequestIdentification(conn)

    def test_answerPrimary3(self):
        """ Answer who's the primary """
        node, conn = self.identifyToMasterNode()
        master_list = self._getMasterList()
        self.election.answerPrimary(conn, node.getUUID(), master_list)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertFalse(self.app.primary)
        self.assertEqual(self.app.primary_master_node, node)
        self.checkRequestIdentification(conn)


class MasterServerElectionTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1)
        self.app = Application(config)
        self.app.pt.clear()
        self.app.name = 'NEOCLUSTER'
        self.app.em = Mock()
        self.election = ServerElectionHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        for node in self.app.nm.getMasterList():
            self.app.unconnected_master_node_set.add(node.getAddress())
            node.setState(NodeStates.RUNNING)
        # define some variable to simulate client and storage node
        self.client_address = (self.local_ip, 1000)
        self.storage_address = (self.local_ip, 2000)
        self.master_address = (self.local_ip, 3000)
        # apply monkey patches
        self._addPacket = ClientConnection._addPacket
        ClientConnection._addPacket = _addPacket

    def tearDown(self):
        NeoUnitTestBase.tearDown(self)
        # restore environnement
        ClientConnection._addPacket = self._addPacket

    def identifyToMasterNode(self, uuid=True):
        node = self.app.nm.getMasterList()[0]
        if uuid is True:
            uuid = self.getNewUUID()
        node.setUUID(uuid)
        conn = self.getFakeConnection(
                uuid=node.getUUID(),
                address=node.getAddress(),
        )
        return (node, conn)


    # Tests

    def test_requestIdentification1(self):
        """ A non-master node request identification """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), node.getAddress(), self.app.name)
        self.assertRaises(protocol.NotReadyError,
            self.election.requestIdentification,
            conn, NodeTypes.CLIENT, *args)

    def test_requestIdentification2(self):
        """ A unknown master node request identification """
        node, conn = self.identifyToMasterNode()
        args = (node.getUUID(), ('127.0.0.1', 1000), self.app.name)
        self.checkProtocolErrorRaised(self.election.requestIdentification,
            conn, NodeTypes.MASTER, *args)

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
        node_type, uuid, partitions, replicas, new_uuid = args
        self.assertEqual(node.getUUID(), new_uuid)
        self.assertNotEqual(node.getUUID(), uuid)

    def test_requestIdentification5(self):
        """ UUID conflict """
        node, conn = self.identifyToMasterNode()
        args = (self.app.uuid, node.getAddress(), self.app.name)
        self.election.requestIdentification(conn,
            NodeTypes.MASTER, *args)
        self.checkUUIDSet(conn)
        args = self.checkAcceptIdentification(conn, decode=True)
        node_type, uuid, partitions, replicas, new_uuid = args
        self.assertNotEqual(self.app.uuid, new_uuid)
        self.assertEqual(self.app.uuid, uuid)


    def _getNodeList(self):
        return [x.asTuple() for x in self.app.nm.getList()]

    def __getClient(self):
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid, address=self.client_address)
        self.app.nm.createClient(uuid=uuid, address=self.client_address)
        return conn

    def __getMaster(self, port=1000, register=True):
        uuid = self.getNewUUID()
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

    def testRequestIdentification2(self):
        """ Check with an unknown master node """
        conn = self.__getMaster(register=False)
        self.checkProtocolErrorRaised(
            self.election.requestIdentification,
            conn=conn,
            node_type=NodeTypes.MASTER,
            uuid=conn.getUUID(),
            address=conn.getAddress(),
            name=self.app.name,
        )

    def testAnnouncePrimary1(self):
        """ check the wrong cases """
        announce = self.election.announcePrimary
        # No uuid
        node, conn = self.identifyToMasterNode(uuid=None)
        self.checkProtocolErrorRaised(announce, conn)
        # Announce to a primary, raise
        self.app.primary = True
        node, conn = self.identifyToMasterNode()
        self.assertTrue(self.app.primary)
        self.assertEqual(self.app.primary_master_node, None)
        self.assertRaises(ElectionFailure, announce, conn)

    def testAnnouncePrimary2(self):
        """ Check the good case """
        announce = self.election.announcePrimary
        # Announce, must set the primary
        self.app.primary = False
        node, conn = self.identifyToMasterNode()
        self.assertFalse(self.app.primary)
        self.assertFalse(self.app.primary_master_node)
        announce(conn)
        self.assertFalse(self.app.primary)
        self.assertEqual(self.app.primary_master_node, node)

    def test_askPrimary1(self):
        """ Ask the primary to the primary """
        node, conn = self.identifyToMasterNode()
        self.app.primary = True
        self.election.askPrimary(conn)
        uuid, master_list = self.checkAnswerPrimary(conn, decode=True)
        self.assertEqual(uuid, self.app.uuid)
        self.assertEqual(len(master_list), 2)
        self.assertEqual(master_list[0], (self.app.server, self.app.uuid))
        master_node = self.app.nm.getMasterList()[0]
        master_node = (master_node.getAddress(), master_node.getUUID())
        self.assertEqual(master_list[1], master_node)

    def test_askPrimary2(self):
        """ Ask the primary to a secondary that known who's te primary """
        node, conn = self.identifyToMasterNode()
        self.app.primary = False
        # it will answer ourself as primary
        self.app.primary_master_node = node
        self.election.askPrimary(conn)
        uuid, master_list = self.checkAnswerPrimary(conn, decode=True)
        self.assertEqual(uuid, node.getUUID())
        self.assertEqual(len(master_list), 2)
        self.assertEqual(master_list[0], (self.app.server, self.app.uuid))
        master_node = (node.getAddress(), node.getUUID())
        self.assertEqual(master_list[1], master_node)

    def test_askPrimary3(self):
        """ Ask the primary to a master that don't known who's the primary """
        node, conn = self.identifyToMasterNode()
        self.app.primary = False
        self.app.primary_master_node = None
        self.election.askPrimary(conn)
        uuid, master_list = self.checkAnswerPrimary(conn, decode=True)
        self.assertEqual(uuid, None)
        self.assertEqual(len(master_list), 2)
        self.assertEqual(master_list[0], (self.app.server, self.app.uuid))
        master_node = self.app.nm.getMasterList()[0]
        master_node = (node.getAddress(), node.getUUID())
        self.assertEqual(master_list[1], master_node)

    def test_reelectPrimary(self):
        node, conn = self.identifyToMasterNode()
        self.assertRaises(ElectionFailure, self.election.reelectPrimary, conn)


if __name__ == '__main__':
    unittest.main()

