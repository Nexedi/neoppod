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
from collections import deque
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.master import MasterOperationHandler
from neo.lib.exception import PrimaryFailure, OperationFailure
from neo.lib.pt import PartitionTable
from neo.lib.protocol import CellStates, ProtocolError, Packets

class StorageMasterHandlerTests(NeoUnitTestBase):

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
        # handler
        self.operation = MasterOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getMasterUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageMasterHandlerTests, self)._tearDown(success)

    def getMasterConnection(self):
        address = ("127.0.0.1", self.master_port)
        return self.getFakeConnection(uuid=self.master_uuid, address=address)

    def test_07_connectionClosed2(self):
        # primary has closed the connection
        conn = self.getMasterConnection()
        self.app.listening_conn = object() # mark as running
        self.assertRaises(PrimaryFailure, self.operation.connectionClosed, conn)
        self.checkNoPacketSent(conn)

    def test_14_notifyPartitionChanges1(self):
        # old partition change -> do nothing
        app = self.app
        conn = self.getMasterConnection()
        app.replicator = Mock({})
        self.app.pt = Mock({'getID': 1})
        count = len(self.app.nm.getList())
        self.operation.notifyPartitionChanges(conn, 0, ())
        self.assertEqual(self.app.pt.getID(), 1)
        self.assertEqual(len(self.app.nm.getList()), count)
        calls = self.app.replicator.mockGetNamedCalls('removePartition')
        self.assertEqual(len(calls), 0)
        calls = self.app.replicator.mockGetNamedCalls('addPartition')
        self.assertEqual(len(calls), 0)

    def test_14_notifyPartitionChanges2(self):
        # cases :
        uuid1, uuid2, uuid3 = [self.getStorageUUID() for i in range(3)]
        cells = (
            (0, uuid1, CellStates.UP_TO_DATE),
            (1, uuid2, CellStates.DISCARDED),
            (2, uuid3, CellStates.OUT_OF_DATE),
        )
        # context
        conn = self.getMasterConnection()
        app = self.app
        # register nodes
        app.nm.createStorage(uuid=uuid1)
        app.nm.createStorage(uuid=uuid2)
        app.nm.createStorage(uuid=uuid3)
        ptid1, ptid2 = (1, 2)
        self.assertNotEqual(ptid1, ptid2)
        app.pt = PartitionTable(3, 1)
        app.dm = Mock({ })
        app.replicator = Mock({})
        self.operation.notifyPartitionChanges(conn, ptid2, cells)
        # ptid set
        self.assertEqual(app.pt.getID(), ptid2)
        # dm call
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(ptid2, cells)

    def test_16_stopOperation1(self):
        # OperationFailure
        conn = self.getFakeConnection(is_server=False)
        self.assertRaises(OperationFailure, self.operation.stopOperation, conn)

    def _getConnection(self):
        return self.getFakeConnection()

    def test_askLockInformation1(self):
        """ Unknown transaction """
        self.app.tm = Mock({'__contains__': False})
        conn = self._getConnection()
        oid_list = [self.getOID(1), self.getOID(2)]
        tid = self.getNextTID()
        ttid = self.getNextTID()
        handler = self.operation
        self.assertRaises(ProtocolError, handler.askLockInformation, conn,
            ttid, tid, oid_list)

    def test_askLockInformation2(self):
        """ Lock transaction """
        self.app.tm = Mock({'__contains__': True})
        conn = self._getConnection()
        tid = self.getNextTID()
        ttid = self.getNextTID()
        oid_list = [self.getOID(1), self.getOID(2)]
        self.operation.askLockInformation(conn, ttid, tid, oid_list)
        calls = self.app.tm.mockGetNamedCalls('lock')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(ttid, tid, oid_list)
        self.checkAnswerInformationLocked(conn)

    def test_notifyUnlockInformation1(self):
        """ Unknown transaction """
        self.app.tm = Mock({'__contains__': False})
        conn = self._getConnection()
        tid = self.getNextTID()
        handler = self.operation
        self.assertRaises(ProtocolError, handler.notifyUnlockInformation,
                conn, tid)

    def test_notifyUnlockInformation2(self):
        """ Unlock transaction """
        self.app.tm = Mock({'__contains__': True})
        conn = self._getConnection()
        tid = self.getNextTID()
        self.operation.notifyUnlockInformation(conn, tid)
        calls = self.app.tm.mockGetNamedCalls('unlock')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)
        self.checkNoPacketSent(conn)

    def test_askPack(self):
        self.app.dm = Mock({'pack': None})
        conn = self.getFakeConnection()
        tid = self.getNextTID()
        self.operation.askPack(conn, tid)
        calls = self.app.dm.mockGetNamedCalls('pack')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid, self.app.tm.updateObjectDataForPack)
        # Content has no meaning here, don't check.
        self.checkAnswerPacket(conn, Packets.AnswerPack)

if __name__ == "__main__":
    unittest.main()
