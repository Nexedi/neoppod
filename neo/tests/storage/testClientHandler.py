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
from neo.storage.transactions import ConflictError, DelayedError
from neo.storage.handlers.client import ClientOperationHandler
from neo.protocol import INVALID_PARTITION
from neo.protocol import INVALID_TID, INVALID_OID, INVALID_SERIAL
from neo.protocol import Packets, LockState

class StorageClientHandlerTests(NeoTestBase):

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
        self.app.tm = Mock({'__contains__': True})
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
        return self.getFakeConnection(uuid=uuid, address=('127.0.0.1', 1000))

    def _checkTransactionsAborted(self, uuid):
        calls = self.app.tm.mockGetNamedCalls('abortFor')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(uuid)

    def test_connectionLost(self):
        uuid = self.getNewUUID()
        self.app.nm.createClient(uuid=uuid)
        conn = self._getConnection(uuid=uuid)
        self.operation.connectionClosed(conn)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self._getConnection()
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_askTransactionInformation2(self):
        # answer
        conn = self._getConnection()
        oid_list = [self.getOID(1), self.getOID(2)]
        dm = Mock({ "getTransaction": (oid_list, 'user', 'desc', '', False), })
        self.app.dm = dm
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkAnswerTransactionInformation(conn)

    def test_24_askObject1(self):
        # delayed response
        conn = self._getConnection()
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
        conn = self._getConnection()
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
        serial = self.getNextTID()
        next_serial = self.getNextTID()
        oid = self.getOID(1)
        tid = self.getNextTID()
        self.app.dm = Mock({'getObject': (serial, next_serial, 0, 0, '', None)})
        conn = self._getConnection()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = self._getConnection()
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.askTIDs(conn, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerTids(conn)

    def test_25_askTIDs3(self):
        # invalid partition => answer usable partitions
        conn = self._getConnection()
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
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn,
            1, 1, None)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_askObjectHistory2(self):
        oid1, oid2 = self.getOID(1), self.getOID(2)
        # first case: empty history
        conn = self._getConnection()
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.askObjectHistory(conn, oid1, 1, 2)
        self.checkAnswerObjectHistory(conn)
        # second case: not empty history
        conn = self._getConnection()
        serial = self.getNextTID()
        self.app.dm = Mock({'getObjectHistory': [(serial, 0, ), ]})
        self.operation.askObjectHistory(conn, oid2, 1, 2)
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
        self._checkStoreObjectCalled(tid, serial, oid, comp,
                checksum, data, None)
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

    def test_askUndoTransaction(self):
        conn = self._getConnection()
        tid = self.getNextTID()
        undone_tid = self.getNextTID()
        oid_1 = self.getNextTID()
        oid_2 = self.getNextTID()
        oid_3 = self.getNextTID()
        oid_4 = self.getNextTID()
        def getTransactionUndoData(tid, undone_tid, getObjectFromTransaction):
            return {
                oid_1: (1, 1),
                oid_2: (1, -1),
                oid_3: (1, 2),
                oid_4: (1, 3),
            }
        self.app.dm.getTransactionUndoData = getTransactionUndoData
        original_storeObject = self.app.tm.storeObject
        def storeObject(tid, serial, oid, *args, **kw):
            if oid == oid_3:
                raise ConflictError(0)
            elif oid == oid_4 and delay_store:
                raise DelayedError
            return original_storeObject(tid, serial, oid, *args, **kw)
        self.app.tm.storeObject = storeObject

        # Check if delaying a store (of oid_4) is supported
        delay_store = True
        self.operation.askUndoTransaction(conn, tid, undone_tid)
        self.checkNoPacketSent(conn)

        delay_store = False
        self.operation.askUndoTransaction(conn, tid, undone_tid)
        oid_list_1, oid_list_2, oid_list_3 = self.checkAnswerPacket(conn,
            Packets.AnswerUndoTransaction, decode=True)
        # Compare sets as order doens't matter here.
        self.assertEqual(set(oid_list_1), set([oid_1, oid_4]))
        self.assertEqual(oid_list_2, [oid_2])
        self.assertEqual(oid_list_3, [oid_3])

    def test_askHasLock(self):
        tid_1 = self.getNextTID()
        tid_2 = self.getNextTID()
        oid = self.getNextTID()
        def getLockingTID(oid):
            return locking_tid
        self.app.tm.getLockingTID = getLockingTID
        for locking_tid, status in (
                    (None, LockState.NOT_LOCKED),
                    (tid_1, LockState.GRANTED),
                    (tid_2, LockState.GRANTED_TO_OTHER),
                ):
            conn = self._getConnection()
            self.operation.askHasLock(conn, tid_1, oid)
            p_oid, p_status = self.checkAnswerPacket(conn,
                Packets.AnswerHasLock, decode=True)
            self.assertEqual(oid, p_oid)
            self.assertEqual(status, p_status)

if __name__ == "__main__":
    unittest.main()
