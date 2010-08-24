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
from collections import deque
from neo.tests import NeoTestBase
from neo.storage.app import Application
from neo.storage.handlers.storage import StorageOperationHandler
from neo.protocol import INVALID_PARTITION
from neo.protocol import INVALID_TID, INVALID_OID, INVALID_SERIAL

class StorageStorageHandlerTests(NeoTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = self.getFakeConnection(address=("127.0.0.1", self.master_port),
                is_server=_listening)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        self.checkUnexpectedPacketRaised(_call, conn=conn, **kwargs)

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        # handler
        self.operation = StorageOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self.getFakeConnection()
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_askTransactionInformation2(self):
        # answer
        conn = self.getFakeConnection()
        tid = self.getNextTID()
        oid_list = [self.getOID(1), self.getOID(2)]
        dm = Mock({"getTransaction": (oid_list, 'user', 'desc', '', False), })
        self.app.dm = dm
        self.operation.askTransactionInformation(conn, tid)
        self.checkAnswerTransactionInformation(conn)

    def test_24_askObject1(self):
        # delayed response
        conn = self.getFakeConnection()
        oid = self.getOID(1)
        tid = self.getNextTID()
        serial = self.getNextTID()
        self.app.dm = Mock()
        self.app.tm = Mock({'loadLocked': True})
        self.app.load_lock_dict[oid] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEquals(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = self.getFakeConnection()
        oid = self.getOID(1)
        tid = self.getNextTID()
        serial = self.getNextTID()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(oid, serial, tid, resolve_data=False)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        oid = self.getOID(1)
        tid = self.getNextTID()
        serial = self.getNextTID()
        next_serial = self.getNextTID()
        # object found => answer
        self.app.dm = Mock({'getObject': (serial, next_serial, 0, 0, '', None)})
        conn = self.getFakeConnection()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDsFrom1(self):
        # well case => answer
        conn = self.getFakeConnection()
        self.app.dm = Mock({'getReplicationTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getPartitions': 1})
        tid = self.getNextTID()
        self.operation.askTIDsFrom(conn, tid, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getReplicationTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(tid, 2, 1, [1, ])
        self.checkAnswerTidsFrom(conn)

    def test_25_askTIDsFrom2(self):
        # invalid partition => answer usable partitions
        conn = self.getFakeConnection()
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getReplicationTIDList': (INVALID_TID, )})
        self.app.pt = Mock({
            'getCellList': (cell, ), 
            'getPartitions': 1,
            'getAssignedPartitionList': [0],
        })
        tid = self.getNextTID()
        self.operation.askTIDsFrom(conn, tid, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getAssignedPartitionList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getReplicationTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(tid, 2, 1, [0, ])
        self.checkAnswerTidsFrom(conn)

    def test_26_askObjectHistoryFrom(self):
        oid = self.getOID(2)
        min_tid = self.getNextTID()
        tid = self.getNextTID()
        conn = self.getFakeConnection()
        self.app.dm = Mock({'getObjectHistoryFrom': [tid]})
        self.operation.askObjectHistoryFrom(conn, oid, min_tid, 2)
        self.checkAnswerObjectHistoryFrom(conn)
        calls = self.app.dm.mockGetNamedCalls('getObjectHistoryFrom')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(oid, min_tid, 2)

    def test_25_askOIDs1(self):
        # well case > answer OIDs
        conn = self.getFakeConnection()
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        oid = self.getOID(1)
        self.operation.askOIDs(conn, oid, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(oid, 2, 1, [1, ])
        self.checkAnswerOids(conn)

    def test_25_askOIDs2(self):
        # invalid partition => answer usable partitions
        conn = self.getFakeConnection()
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        self.app.pt = Mock({
            'getCellList': (cell, ), 
            'getPartitions': 1,
            'getAssignedPartitionList': [0],
        })
        oid = self.getOID(1)
        self.operation.askOIDs(conn, oid, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getAssignedPartitionList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(oid, 2, 1, [0])
        self.checkAnswerOids(conn)


if __name__ == "__main__":
    unittest.main()
