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
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, NodeStates, Packets
from neo.master.handlers.client import ClientServiceHandler
from neo.master.app import Application

class MasterClientHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.pt.setID(1)
        self.app.em = Mock()
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
        self.storage_uuid = self.getStorageUUID()
        # register the storage
        self.app.nm.createStorage(
            uuid=self.storage_uuid,
            address=self.storage_address,
        )

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN """
        # register the master itself
        uuid = self.getNewUUID(node_type)
        self.app.nm.createFromNodeType(
            node_type,
            address=(ip, port),
            uuid=uuid,
            state=NodeStates.RUNNING,
        )
        return uuid

    # Tests
    def test_07_askBeginTransaction(self):
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        service = self.service
        tm_org = self.app.tm
        self.app.tm = tm = Mock({
            'begin': '\x00\x00\x00\x00\x00\x00\x00\x01',
        })
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        client_node = self.app.nm.getByUUID(client_uuid)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askBeginTransaction(conn, None)
        calls = tm.mockGetNamedCalls('begin')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(client_node, None)
        self.checkAnswerBeginTransaction(conn)
        # Client asks for a TID
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.tm = tm_org
        service.askBeginTransaction(conn, tid1)
        calls = tm.mockGetNamedCalls('begin')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(client_node, None)
        args = self.checkAnswerBeginTransaction(conn, decode=True)
        self.assertEqual(args, (tid1, ))

    def test_08_askNewOIDs(self):
        service = self.service
        oid1, oid2 = self.getOID(1), self.getOID(2)
        self.app.tm.setLastOID(oid1)
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        for node in self.app.nm.getStorageList():
            conn = self.getFakeConnection(node.getUUID(), node.getAddress())
            node.setConnection(conn)
        service.askNewOIDs(conn, 1)
        self.assertTrue(self.app.tm.getLastOID() > oid1)

    def test_09_askFinishTransaction(self):
        service = self.service
        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        storage_uuid = self.storage_uuid
        storage_conn = self.getFakeConnection(storage_uuid, self.storage_address)
        storage2_uuid = self.identifyToMasterNode(port=10022)
        storage2_conn = self.getFakeConnection(storage2_uuid,
            (self.storage_address[0], self.storage_address[1] + 1))
        self.app.setStorageReady(storage2_uuid)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.pt = Mock({
            'getPartition': 0,
            'getCellList': [
                Mock({'getUUID': storage_uuid}),
                Mock({'getUUID': storage2_uuid}),
            ],
            'getPartitions': 2,
        })
        ttid = self.getNextTID()
        service.askBeginTransaction(conn, ttid)
        oid_list = []
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.nm.getByUUID(storage_uuid).setConnection(storage_conn)
        # No packet sent if storage node is not ready
        self.assertFalse(self.app.isStorageReady(storage_uuid))
        service.askFinishTransaction(conn, ttid, oid_list)
        self.checkNoPacketSent(storage_conn)
        self.app.tm.abortFor(self.app.nm.getByUUID(client_uuid))
        # ...but AskLockInformation is sent if it is ready
        self.app.setStorageReady(storage_uuid)
        self.assertTrue(self.app.isStorageReady(storage_uuid))
        service.askFinishTransaction(conn, ttid, oid_list)
        self.checkAskLockInformation(storage_conn)
        self.assertEqual(len(self.app.tm.registerForNotification(storage_uuid)), 1)
        txn = self.app.tm[ttid]
        pending_ttid = list(self.app.tm.registerForNotification(storage_uuid))[0]
        self.assertEqual(ttid, pending_ttid)
        self.assertEqual(len(txn.getOIDList()), 0)
        self.assertEqual(len(txn.getUUIDList()), 1)

    def test_askNodeInformations(self):
        # check that only informations about master and storages nodes are
        # send to a client
        self.app.nm.createClient()
        conn = self.getFakeConnection()
        self.service.askNodeInformation(conn)
        calls = conn.mockGetNamedCalls('notify')
        self.assertEqual(len(calls), 1)
        packet = calls[0].getParam(0)
        (node_list, ) = packet.decode()
        self.assertEqual(len(node_list), 2)

    def test_connectionClosed(self):
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.listening_conn = object() # mark as running
        lptid = self.app.pt.getID()
        self.assertEqual(self.app.nm.getByUUID(client_uuid).getState(),
                NodeStates.RUNNING)
        self.service.connectionClosed(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEqual(self.app.nm.getByUUID(client_uuid), None)
        self.assertEqual(lptid, self.app.pt.getID())

    def test_askPack(self):
        self.assertEqual(self.app.packing, None)
        self.app.nm.createClient()
        tid = self.getNextTID()
        peer_id = 42
        conn = self.getFakeConnection(peer_id=peer_id)
        storage_uuid = self.storage_uuid
        storage_conn = self.getFakeConnection(storage_uuid,
            self.storage_address)
        self.app.nm.getByUUID(storage_uuid).setConnection(storage_conn)
        self.service.askPack(conn, tid)
        self.checkNoPacketSent(conn)
        ptid = self.checkAskPacket(storage_conn, Packets.AskPack,
            decode=True)[0]
        self.assertEqual(ptid, tid)
        self.assertTrue(self.app.packing[0] is conn)
        self.assertEqual(self.app.packing[1], peer_id)
        self.assertEqual(self.app.packing[2], {storage_uuid})
        # Asking again to pack will cause an immediate error
        storage_uuid = self.identifyToMasterNode(port=10022)
        storage_conn = self.getFakeConnection(storage_uuid,
            self.storage_address)
        self.app.nm.getByUUID(storage_uuid).setConnection(storage_conn)
        self.service.askPack(conn, tid)
        self.checkNoPacketSent(storage_conn)
        status = self.checkAnswerPacket(conn, Packets.AnswerPack,
            decode=True)[0]
        self.assertFalse(status)

if __name__ == '__main__':
    unittest.main()

