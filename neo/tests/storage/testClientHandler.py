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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import os
import unittest
from neo import logging
from struct import pack, unpack
from mock import Mock
from collections import deque
from neo.tests import NeoTestBase
from neo.storage.app import Application
from neo.storage.handlers.client import TransactionInformation
from neo.storage.handlers.client import ClientOperationHandler
from neo.exception import PrimaryFailure, OperationFailure
from neo.pt import PartitionTable
from neo.protocol import Packets, Packet, INVALID_PARTITION
from neo.protocol import INVALID_TID, INVALID_OID, INVALID_SERIAL

class StorageClientHandlerTests(NeoTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = Mock({ 
            "getAddress" : ("127.0.0.1", self.master_port), 
            "isServer": _listening,    
        })
        packet = Packet(msg_type=_msg_type)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        self.checkUnexpectedPacketRaised(_call, conn=conn, packet=packet, **kwargs)

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(**config)
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
        # handler
        self.operation = ClientOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def test_01_TransactionInformation(self):
        uuid = self.getNewUUID()
        transaction = TransactionInformation(uuid)
        # uuid
        self.assertEquals(transaction._uuid, uuid)
        self.assertEquals(transaction.getUUID(), uuid)
        # objects
        self.assertEquals(transaction._object_dict, {})
        object = (self.getNewUUID(), 1, 2, 3, )
        transaction.addObject(*object)
        objects = transaction.getObjectList()
        self.assertEquals(len(objects), 1)
        self.assertEquals(objects[0], object)
        # transactions
        self.assertEquals(transaction._transaction, None)
        t = ((1, 2, 3), 'user', 'desc', '')
        transaction.addTransaction(*t)
        self.assertEquals(transaction.getTransaction(), t)

    def test_05_dealWithClientFailure(self):
        # check if client's transaction are cleaned
        uuid = self.getNewUUID()
        client = self.app.nm.createClient(
            uuid=uuid,
            address=('127.0.0.1', 10010)
        )
        self.app.store_lock_dict[0] = object()
        transaction = Mock({
            'getUUID': uuid,
            'getObjectList': ((0, ), ),
        })
        self.app.transaction_dict[0] = transaction
        self.assertTrue(1 not in self.app.store_lock_dict)
        self.assertTrue(1 not in self.app.transaction_dict)
        self.operation.dealWithClientFailure(uuid)
        # objects and transaction removed
        self.assertTrue(0 not in self.app.store_lock_dict)
        self.assertTrue(0 not in self.app.transaction_dict)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = Mock({ })
        packet = Packets.AskTransactionInformation()
        packet.setId(0)
        self.operation.askTransactionInformation(conn, packet, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_askTransactionInformation2(self):
        # answer
        conn = Mock({ })
        packet = Packets.AskTransactionInformation()
        packet.setId(0)
        dm = Mock({ "getTransaction": (INVALID_TID, 'user', 'desc', '', ), })
        self.app.dm = dm
        self.operation.askTransactionInformation(conn, packet, INVALID_TID)
        self.checkAnswerTransactionInformation(conn)

    def test_24_askObject1(self):
        # delayed response
        conn = Mock({})
        self.app.dm = Mock()
        packet = Packets.AskObject()
        packet.setId(0)
        self.app.load_lock_dict[INVALID_OID] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = Mock({})
        packet = Packets.AskObject()
        packet.setId(0)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(INVALID_OID, INVALID_TID, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        # object found => answer
        self.app.dm = Mock({'getObject': ('', '', 0, 0, '', )})
        conn = Mock({})
        packet = Packets.AskObject()
        packet.setId(0)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packets.AskTIDs()
        packet.setId(0)
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, packet, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = Mock({})
        packet = Packets.AskTIDs()
        packet.setId(0)
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.askTIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerTids(conn)

    def test_25_askTIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packets.AskTIDs()
        packet.setId(0)
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getCellList': (cell, ), 'getPartitions': 1})
        self.operation.askTIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [0, ])
        self.checkAnswerTids(conn)

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = Mock({})
        packet = Packets.AskObjectHistory()
        packet.setId(0)
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn, packet, 1, 1, None)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_askObjectHistory2(self):
        # first case: empty history
        packet = Packets.AskObjectHistory()
        packet.setId(0)
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.askObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)
        # second case: not empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': [('', 0, ), ]})
        self.operation.askObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)

    def test_27_askStoreTransaction2(self):
        # add transaction entry
        packet = Packets.AskStoreTransaction()
        packet.setId(0)
        conn = Mock({'getUUID': self.getNewUUID()})
        self.operation.askStoreTransaction(conn, packet,
            INVALID_TID, '', '', '', ())
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertTrue(isinstance(t, TransactionInformation))
        self.assertEquals(t.getTransaction(), ((), '', '', ''))
        self.checkAnswerStoreTransaction(conn)

    def test_28_askStoreObject2(self):
        # locked => delayed response
        packet = Packets.AskStoreObject()
        packet.setId(0)
        conn = Mock({'getUUID': self.app.uuid})
        oid = '\x02' * 8
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[oid] = tid1
        self.assertTrue(oid in self.app.store_lock_dict)
        t_before = self.app.transaction_dict.items()[:]
        self.operation.askStoreObject(conn, packet, oid, 
            INVALID_SERIAL, 0, 0, '', tid2)
        self.assertEquals(len(self.app.event_queue), 1)
        t_after = self.app.transaction_dict.items()[:]
        self.assertEquals(t_before, t_after)
        self.checkNoPacketSent(conn)
        self.assertTrue(oid in self.app.store_lock_dict)

    def test_28_askStoreObject3(self):
        # locked => unresolvable conflict => answer
        packet = Packets.AskStoreObject()
        packet.setId(0)
        conn = Mock({'getUUID': self.app.uuid})
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[INVALID_OID] = tid2
        self.operation.askStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', tid1)
        self.checkAnswerStoreObject(conn)
        self.assertEquals(self.app.store_lock_dict[INVALID_OID], tid2)
        # conflicting
        packet = conn.mockGetNamedCalls('answer')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
    
    def test_28_askStoreObject4(self):
        # resolvable conflict => answer
        packet = Packets.AskStoreObject()
        packet.setId(0)
        conn = Mock({'getUUID': self.app.uuid})
        self.app.dm = Mock({'getObjectHistory':((self.getNewUUID(), ), )})
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        self.operation.askStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        self.checkAnswerStoreObject(conn)
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        # conflicting
        packet = conn.mockGetNamedCalls('answer')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
        
    def test_28_askStoreObject5(self):
        # no conflict => answer
        packet = Packets.AskStoreObject()
        packet.setId(0)
        conn = Mock({'getUUID': self.app.uuid})
        self.operation.askStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertEquals(len(t.getObjectList()), 1)
        object = t.getObjectList()[0]
        self.assertEquals(object, (INVALID_OID, 0, 0, ''))
        # no conflict
        packet = self.checkAnswerStoreObject(conn)
        self.assertFalse(unpack('!B8s8s', packet._body)[0])

    def test_29_abortTransaction(self):
        # remove transaction
        packet = Packets.AbortTransaction()
        packet.setId(0)
        conn = Mock({'getUUID': self.app.uuid})
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        self.called = False
        def called():
            self.called = True
        self.app.executeQueuedEvents = called
        self.app.load_lock_dict[0] = object()
        self.app.store_lock_dict[0] = object()
        self.app.transaction_dict[INVALID_TID] = transaction
        self.operation.abortTransaction(conn, packet, INVALID_TID)
        self.assertTrue(self.called)
        self.assertEquals(len(self.app.load_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)

if __name__ == "__main__":
    unittest.main()
