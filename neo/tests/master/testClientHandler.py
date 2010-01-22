#
# Copyright (C) 2009  Nexedi SA
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
from struct import pack, unpack
from neo.tests import NeoTestBase
from neo.protocol import Packets, NodeTypes, NodeStates
from neo.master.handlers.client import ClientServiceHandler
from neo.master.app import Application

class MasterClientHandlerTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.pt.clear()
        self.app.pt.setID(pack('!Q', 1))
        self.app.em = Mock({"getConnectionList" : []})
        self.app.loid = '\0' * 8
        self.app.tm.setLastTID('\0' * 8)
        self.service = ClientServiceHandler(self.app)
        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10010
        self.master_address = ('127.0.0.1', self.master_port)
        self.client_address = ('127.0.0.1', self.client_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        # register the storage
        kw = {'uuid':self.getNewUUID(), 'address': self.master_address}
        self.app.nm.createStorage(**kw)

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def getLastUUID(self):
        return self.uuid

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN """
        # register the master itself
        uuid = self.getNewUUID()
        self.app.nm.createFromNodeType(
            node_type,
            address=(ip, port),
            uuid=uuid,
            state=NodeStates.RUNNING,
        )
        return uuid

    # Tests
    def test_07_askBeginTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AskBeginTransaction()
        packet.setId(0)
        ltid = self.app.tm.getLastTID()
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askBeginTransaction(conn, packet, None)
        self.failUnless(ltid < self.app.tm.getLastTID())
        self.assertEqual(len(self.app.tm.getPendingList()), 1)
        tid = self.app.tm.getPendingList()[0]
        self.assertEquals(tid, self.app.tm.getLastTID())

    def test_08_askNewOIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AskNewOIDs()
        packet.setId(0)
        loid = self.app.loid
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askNewOIDs(conn, packet, 1)
        self.failUnless(loid < self.app.loid)

    def test_09_finishTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.FinishTransaction()
        packet.setId(9)
        # give an older tid than the PMN known, must abort
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        oid_list = []
        upper, lower = unpack('!LL', self.app.tm.getLastTID())
        new_tid = pack('!LL', upper, lower + 10)
        self.checkUnexpectedPacketRaised(service.finishTransaction, conn, packet, oid_list, new_tid)
        old_node = self.app.nm.getByUUID(uuid)
        self.app.nm.remove(old_node)
        self.app.pt.dropNode(old_node)

        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        storage_uuid = self.identifyToMasterNode()
        storage_conn = self.getFakeConnection(storage_uuid, self.storage_address)
        self.assertNotEquals(uuid, client_uuid)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askBeginTransaction(conn, packet, None)
        oid_list = []
        tid = self.app.tm.getLastTID()
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn]})
        service.finishTransaction(conn, packet, oid_list, tid)
        self.checkLockInformation(storage_conn)
        self.assertEquals(len(self.app.tm.getPendingList()), 1)
        apptid = self.app.tm.getPendingList()[0]
        self.assertEquals(tid, apptid)
        txn = self.app.tm[tapptid]
        self.assertEquals(len(txn.getOIDList()), 0)
        self.assertEquals(len(txn.getUUIDSet()), 1)
        self.assertEquals(txn.getMessageId(), 9)


    def test_11_abortTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AbortTransaction()
        # give a bad tid, must not failed, just ignored it
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.assertFalse(self.app.tm.hasPending())
        service.abortTransaction(conn, packet, None)
        self.assertFalse(self.app.tm.hasPending())
        # give a known tid
        conn = self.getFakeConnection(client_uuid, self.client_address)
        tid = self.app.tm.getLastTID()
        self.app.tm.remove(tid)
        self.app.tm.begin(Mock({'__hash__': 1}), tid)
        self.assertTrue(self.app.tm.hasPending())
        service.abortTransaction(conn, packet, tid)
        self.assertFalse(self.app.tm.hasPending())

    def __testWithMethod(self, method, state):
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = Packets.AskBeginTransaction()
        packet.setId(0)
        self.service.askBeginTransaction(conn, packet, None)
        self.service.askBeginTransaction(conn, packet, None)
        self.service.askBeginTransaction(conn, packet, None)
        self.assertEquals(self.app.nm.getByUUID(client_uuid).getState(),
                NodeStates.RUNNING)
        self.assertEquals(len(self.app.tm.getPendingList()), 3)
        method(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getByUUID(client_uuid), None)
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertFalse(self.app.tm.hasPending())

    def test_15_peerBroken(self):
        self.__testWithMethod(self.service.peerBroken, NodeStates.BROKEN)

    def test_16_timeoutExpired(self):
        self.__testWithMethod(self.service.timeoutExpired,
                NodeStates.TEMPORARILY_DOWN)

    def test_17_connectionClosed(self):
        self.__testWithMethod(self.service.connectionClosed,
            NodeStates.TEMPORARILY_DOWN)


if __name__ == '__main__':
    unittest.main()

