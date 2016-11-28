#
# Copyright (C) 2009-2016  Nexedi SA
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
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.client import ClientOperationHandler
from neo.lib.protocol import INVALID_TID, INVALID_OID, Packets, LockState

class StorageClientHandlerTests(NeoUnitTestBase):

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

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageClientHandlerTests, self)._tearDown(success)

    def _getConnection(self, uuid=None):
        return self.getFakeConnection(uuid=uuid, address=('127.0.0.1', 1000))

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self._getConnection()
        self.app.dm = Mock({'getNumPartitions': 1})
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

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

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn,
            1, 1, None)
        self.assertEqual(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

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
