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
from neo.storage.transactions import ConflictError
from neo.storage.handlers.client import ClientOperationHandler
from neo.protocol import INVALID_PARTITION
from neo.protocol import INVALID_TID, INVALID_OID, INVALID_SERIAL

class StorageClientHandlerTests(NeoTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = Mock({
            "getAddress" : ("127.0.0.1", self.master_port),
            "isServer": _listening,
        })
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
        self.app.tm = Mock()
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

    def _getConnection(self, uuid=None):
        return Mock({'getUUID': uuid, 'getAddress': ('127.0.0.1', 1000)})

    def _checkTransactionsAborted(self, uuid):
        calls = self.app.tm.mockGetNamedCalls('abortFor')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(uuid)

    def test_connectionClosed(self):
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        self.operation.connectionClosed(conn)
        self._checkTransactionsAborted(uuid)

    def test_timeoutExpired(self):
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        self.operation.connectionClosed(conn)
        self._checkTransactionsAborted(uuid)

    def test_peerBroken(self):
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        self.operation.connectionClosed(conn)
        self._checkTransactionsAborted(uuid)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = Mock({ })
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_askTransactionInformation2(self):
        # answer
        conn = Mock({ })
        dm = Mock({ "getTransaction": (INVALID_TID, 'user', 'desc', '', False), })
        self.app.dm = dm
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkAnswerTransactionInformation(conn)

    def test_24_askObject1(self):
        # delayed response
        conn = Mock({})
        self.app.dm = Mock()
        self.app.tm = Mock({'loadLocked': True})
        self.app.load_lock_dict[INVALID_OID] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=INVALID_OID,
            serial=INVALID_SERIAL, tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = Mock({})
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=INVALID_OID,
            serial=INVALID_SERIAL, tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(INVALID_OID, INVALID_TID, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        # object found => answer
        self.app.dm = Mock({'getObject': ('', '', 0, 0, '', )})
        conn = Mock({})
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=INVALID_OID,
            serial=INVALID_SERIAL, tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = Mock({})
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.askTIDs(conn, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerTids(conn)

    def test_25_askTIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({
            'getCellList': (cell, ), 
            'getPartitions': 1,
            'getAssignedPartitionList': [0],
        })
        self.operation.askTIDs(conn, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getAssignedPartitionList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [0])
        self.checkAnswerTids(conn)

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = Mock({})
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn,
            1, 1, None)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_askObjectHistory2(self):
        # first case: empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.askObjectHistory(conn, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)
        # second case: not empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': [('', 0, ), ]})
        self.operation.askObjectHistory(conn, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)

    def test_askStoreTransaction(self):
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        tid = self.getNextTID()
        user = 'USER'
        desc = 'DESC'
        ext = 'EXT'
        oid_list = (self.getOID(1), self.getOID(2))
        self.operation.askStoreTransaction(conn, tid, user, desc, ext, oid_list)
        calls = self.app.tm.mockGetNamedCalls('storeTransaction')
        self.assertEqual(len(calls), 1)
        self.checkAnswerStoreTransaction(conn)

    def _getObject(self):
        oid = self.getOID(0)
        serial = self.getNextTID()
        return (oid, serial, 1, '1', 'DATA')

    def _checkStoreObjectCalled(self, *args):
        calls = self.app.tm.mockGetNamedCalls('storeObject')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(*args)

    def test_askStoreObject1(self):
        # no conflict => answer
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        tid = self.getNextTID()
        oid, serial, comp, checksum, data = self._getObject()
        self.operation.askStoreObject(conn, oid, serial, comp, checksum, 
                data, tid)
        self._checkStoreObjectCalled(uuid, tid, serial, oid, comp,
                checksum, data)
        self.checkAnswerStoreObject(conn)

    def test_askStoreObject2(self):
        # conflict error
        uuid = self.getNewUUID()
        conn = self._getConnection(uuid=uuid)
        tid = self.getNextTID()
        locking_tid = self.getNextTID(tid)
        def fakeStoreObject(*args):
            raise ConflictError(locking_tid)
        self.app.tm.storeObject = lambda *kw: fakeStoreObject
        oid, serial, comp, checksum, data = self._getObject()
        self.operation.askStoreObject(conn, oid, serial, comp, checksum, 
                data, tid)
        self.checkAnswerStoreObject(conn)

    def test_abortTransaction(self):
        conn = self._getConnection()
        tid = self.getNextTID()
        self.operation.abortTransaction(conn, tid)
        calls = self.app.tm.mockGetNamedCalls('abort')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)

if __name__ == "__main__":
    unittest.main()
