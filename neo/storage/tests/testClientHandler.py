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
import logging
from struct import pack, unpack
from mock import Mock
from collections import deque
from neo.tests.base import NeoTestBase
from neo.master.app import MasterNode
from neo.storage.app import Application, StorageNode
from neo.storage.handlers.client import TransactionInformation
from neo.storage.handlers import ClientOperationHandler
from neo.exception import PrimaryFailure, OperationFailure
from neo.pt import PartitionTable
from neo import protocol
from neo.protocol import *

class StorageClientHandlerTests(NeoTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = Mock({ 
            "getAddress" : ("127.0.0.1", self.master_port), 
            "isServerConnection": _listening,    
        })
        packet = Packet(msg_type=_msg_type)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        self.checkUnexpectedPacketRaised(_call, conn=conn, packet=packet, **kwargs)

    def setUp(self):
        logging.basicConfig(level = logging.ERROR)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getConfigFile(master_number=1)
        self.app = Application(config, "storage1")
        self.app.num_partitions = 1
        self.app.num_replicas = 1
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        for server in self.app.master_node_list:
            master = MasterNode(server = server)
            self.app.nm.add(master)
        # handler
        self.operation = ClientOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterNodeList()[0]
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
        from neo.node import ClientNode
        client = ClientNode(('127.0.0.1', 10010))
        client.setUUID(uuid)
        self.app.nm.add(client)
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

    def test_18_handleAskTransactionInformation1(self):
        # transaction does not exists
        conn = Mock({ })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.operation.handleAskTransactionInformation(conn, packet, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_handleAskTransactionInformation2(self):
        # answer
        conn = Mock({ })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        dm = Mock({ "getTransaction": (INVALID_TID, 'user', 'desc', '', ), })
        self.app.dm = dm
        self.operation.handleAskTransactionInformation(conn, packet, INVALID_TID)
        self.checkAnswerTransactionInformation(conn)

    def test_24_handleAskObject1(self):
        # delayed response
        conn = Mock({})
        self.app.dm = Mock()
        packet = Packet(msg_type=ASK_OBJECT)
        self.app.load_lock_dict[INVALID_OID] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_handleAskObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = Mock({})
        packet = Packet(msg_type=ASK_OBJECT)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(INVALID_OID, None, None)
        self.checkErrorPacket(conn)

    def test_24_handleAskObject3(self):
        # object found => answer
        self.app.dm = Mock({'getObject': ('', '', 0, 0, '', )})
        conn = Mock({})
        packet = Packet(msg_type=ASK_OBJECT)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_handleAskTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packet(msg_type=ASK_TIDS)
        self.checkProtocolErrorRaised(self.operation.handleAskTIDs, conn, packet, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_handleAskTIDs2(self):
        # well case => answer
        conn = Mock({})
        packet = Packet(msg_type=ASK_TIDS)
        self.app.num_partitions = 1
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.handleAskTIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerTids(conn)

    def test_25_handleAskTIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packet(msg_type=ASK_TIDS)
        self.app.num_partitions = 1
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getCellList': (cell, )})
        self.operation.handleAskTIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [0, ])
        self.checkAnswerTids(conn)

    def test_26_handleAskObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = Mock({})
        packet = Packet(msg_type=ASK_OBJECT_HISTORY)
        self.checkProtocolErrorRaised(self.operation.handleAskObjectHistory, conn, packet, 1, 1, None)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_handleAskObjectHistory2(self):
        # first case: empty history
        packet = Packet(msg_type=ASK_OBJECT_HISTORY)
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.handleAskObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)
        # second case: not empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': [('', 0, ), ]})
        self.operation.handleAskObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)

    def test_27_handleAskStoreTransaction2(self):
        # add transaction entry
        packet = Packet(msg_type=ASK_STORE_TRANSACTION)
        conn = Mock({'getUUID': self.getNewUUID()})
        self.operation.handleAskStoreTransaction(conn, packet,
            INVALID_TID, '', '', '', ())
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertTrue(isinstance(t, TransactionInformation))
        self.assertEquals(t.getTransaction(), ((), '', '', ''))
        self.checkAnswerStoreTransaction(conn)

    def test_28_handleAskStoreObject2(self):
        # locked => delayed response
        packet = Packet(msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        oid = '\x02' * 8
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[oid] = tid1
        self.assertTrue(oid in self.app.store_lock_dict)
        t_before = self.app.transaction_dict.items()[:]
        self.operation.handleAskStoreObject(conn, packet, oid, 
            INVALID_SERIAL, 0, 0, '', tid2)
        self.assertEquals(len(self.app.event_queue), 1)
        t_after = self.app.transaction_dict.items()[:]
        self.assertEquals(t_before, t_after)
        self.checkNoPacketSent(conn)
        self.assertTrue(oid in self.app.store_lock_dict)

    def test_28_handleAskStoreObject3(self):
        # locked => unresolvable conflict => answer
        packet = Packet(msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[INVALID_OID] = tid2
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', tid1)
        self.checkAnswerStoreObject(conn)
        self.assertEquals(self.app.store_lock_dict[INVALID_OID], tid2)
        # conflicting
        packet = conn.mockGetNamedCalls('answer')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
    
    def test_28_handleAskStoreObject4(self):
        # resolvable conflict => answer
        packet = Packet(msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        self.app.dm = Mock({'getObjectHistory':((self.getNewUUID(), ), )})
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        self.checkAnswerStoreObject(conn)
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        # conflicting
        packet = conn.mockGetNamedCalls('answer')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
        
    def test_28_handleAskStoreObject5(self):
        # no conflict => answer
        packet = Packet(msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertEquals(len(t.getObjectList()), 1)
        object = t.getObjectList()[0]
        self.assertEquals(object, (INVALID_OID, 0, 0, ''))
        # no conflict
        packet = self.checkAnswerStoreObject(conn)
        self.assertFalse(unpack('!B8s8s', packet._body)[0])

    def test_29_handleAbortTransaction(self):
        # remove transaction
        packet = Packet(msg_type=ABORT_TRANSACTION)
        conn = Mock({'getUUID': self.app.uuid})
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        self.called = False
        def called():
            self.called = True
        self.app.executeQueuedEvents = called
        self.app.load_lock_dict[0] = object()
        self.app.store_lock_dict[0] = object()
        self.app.transaction_dict[INVALID_TID] = transaction
        self.operation.handleAbortTransaction(conn, packet, INVALID_TID)
        self.assertTrue(self.called)
        self.assertEquals(len(self.app.load_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)

if __name__ == "__main__":
    unittest.main()
