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
from mock import Mock, ReturnValues
from collections import deque
from neo.lib.util import makeChecksum
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.transactions import ConflictError
from neo.storage.handlers.client import ClientOperationHandler
from neo.lib.protocol import INVALID_PARTITION, INVALID_TID, INVALID_OID
from neo.lib.protocol import Packets, LockState, ZERO_HASH

class StorageClientHandlerTests(NeoUnitTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = self.getFakeConnection(address=("127.0.0.1", self.master_port),
                is_server=_listening)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        self.checkUnexpectedPacketRaised(_call, conn=conn, **kwargs)

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        self.app.event_queue_dict = {}
        self.app.tm = Mock({'__contains__': True})
        # handler
        self.operation = ClientOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getMasterUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageClientHandlerTests, self)._tearDown(success)

    def _getConnection(self, uuid=None):
        return self.getFakeConnection(uuid=uuid, address=('127.0.0.1', 1000))

    def _checkTransactionsAborted(self, uuid):
        calls = self.app.tm.mockGetNamedCalls('abortFor')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(uuid)

    def test_connectionLost(self):
        uuid = self.getClientUUID()
        self.app.nm.createClient(uuid=uuid)
        conn = self._getConnection(uuid=uuid)
        self.operation.connectionClosed(conn)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self._getConnection()
        self.app.dm = Mock({'getNumPartitions': 1})
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
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=INVALID_OID,
            serial=INVALID_TID, tid=INVALID_TID)
        self.assertEqual(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEqual(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = self._getConnection()
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=INVALID_OID,
            serial=INVALID_TID, tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEqual(len(self.app.event_queue), 0)
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(INVALID_OID, INVALID_TID, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        # object found => answer
        serial = self.getNextTID()
        next_serial = self.getNextTID()
        oid = self.getOID(1)
        tid = self.getNextTID()
        H = "0" * 20
        self.app.dm = Mock({'getObject': (serial, next_serial, 0, H, '', None)})
        conn = self._getConnection()
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEqual(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, 1, 1, None)
        self.assertEqual(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEqual(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = self._getConnection()
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.askTIDs(conn, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(1, 1, [1, ])
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
        self.assertEqual(len(self.app.pt.mockGetNamedCalls('getAssignedPartitionList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(1, 1, [0])
        self.checkAnswerTids(conn)

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn,
            1, 1, None)
        self.assertEqual(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_askObjectHistory2(self):
        oid1, oid2 = self.getOID(1), self.getOID(2)
        # first case: empty history
        conn = self._getConnection()
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.askObjectHistory(conn, oid1, 1, 2)
        self.checkErrorPacket(conn)
        # second case: not empty history
        conn = self._getConnection()
        serial = self.getNextTID()
        self.app.dm = Mock({'getObjectHistory': [(serial, 0, ), ]})
        self.operation.askObjectHistory(conn, oid2, 1, 2)
        self.checkAnswerObjectHistory(conn)

    def test_askStoreTransaction(self):
        conn = self._getConnection(uuid=self.getClientUUID())
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
        data = 'DATA'
        return (oid, serial, 1, makeChecksum(data), data)

    def _checkStoreObjectCalled(self, *args):
        calls = self.app.tm.mockGetNamedCalls('storeObject')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(*args)

    def test_askStoreObject1(self):
        # no conflict => answer
        conn = self._getConnection(uuid=self.getClientUUID())
        tid = self.getNextTID()
        oid, serial, comp, checksum, data = self._getObject()
        self.operation.askStoreObject(conn, oid, serial, comp, checksum,
                data, None, tid, False)
        self._checkStoreObjectCalled(tid, serial, oid, comp,
                checksum, data, None, False)
        pconflicting, poid, pserial = self.checkAnswerStoreObject(conn,
            decode=True)
        self.assertEqual(pconflicting, 0)
        self.assertEqual(poid, oid)
        self.assertEqual(pserial, serial)

    def test_askStoreObjectWithDataTID(self):
        # same as test_askStoreObject1, but with a non-None data_tid value
        conn = self._getConnection(uuid=self.getClientUUID())
        tid = self.getNextTID()
        oid, serial, comp, checksum, data = self._getObject()
        data_tid = self.getNextTID()
        self.operation.askStoreObject(conn, oid, serial, comp, ZERO_HASH,
                '', data_tid, tid, False)
        self._checkStoreObjectCalled(tid, serial, oid, comp,
                None, None, data_tid, False)
        pconflicting, poid, pserial = self.checkAnswerStoreObject(conn,
            decode=True)
        self.assertEqual(pconflicting, 0)
        self.assertEqual(poid, oid)
        self.assertEqual(pserial, serial)

    def test_askStoreObject2(self):
        # conflict error
        conn = self._getConnection(uuid=self.getClientUUID())
        tid = self.getNextTID()
        locking_tid = self.getNextTID(tid)
        def fakeStoreObject(*args):
            raise ConflictError(locking_tid)
        self.app.tm.storeObject = fakeStoreObject
        oid, serial, comp, checksum, data = self._getObject()
        self.operation.askStoreObject(conn, oid, serial, comp, checksum,
                data, None, tid, False)
        pconflicting, poid, pserial = self.checkAnswerStoreObject(conn,
            decode=True)
        self.assertEqual(pconflicting, 1)
        self.assertEqual(poid, oid)
        self.assertEqual(pserial, locking_tid)

    def test_abortTransaction(self):
        conn = self._getConnection()
        tid = self.getNextTID()
        self.operation.abortTransaction(conn, tid)
        calls = self.app.tm.mockGetNamedCalls('abort')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)

    def test_askObjectUndoSerial(self):
        conn = self._getConnection(uuid=self.getClientUUID())
        tid = self.getNextTID()
        ltid = self.getNextTID()
        undone_tid = self.getNextTID()
        # Keep 2 entries here, so we check findUndoTID is called only once.
        oid_list = [self.getOID(1), self.getOID(2)]
        obj2_data = [] # Marker
        self.app.tm = Mock({
            'getObjectFromTransaction': None,
        })
        self.app.dm = Mock({
            'findUndoTID': ReturnValues((None, None, False), )
        })
        self.operation.askObjectUndoSerial(conn, tid, ltid, undone_tid, oid_list)
        self.checkErrorPacket(conn)

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
