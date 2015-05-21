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
from neo.lib.pt import PartitionTable
from neo.storage.app import Application
from neo.storage.handlers.verification import VerificationHandler
from neo.lib.protocol import CellStates, ErrorCodes
from neo.lib.exception import PrimaryFailure
from neo.lib.util import p64, u64

class StorageVerificationHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.verification = VerificationHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.client_port = 11011
        self.num_partitions = 1009
        self.num_replicas = 2
        self.app.operational = False
        self.app.load_lock_dict = {}
        self.app.pt = PartitionTable(self.num_partitions, self.num_replicas)

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageVerificationHandlerTests, self)._tearDown(success)

    # Common methods
    def getMasterConnection(self):
        return self.getFakeConnection(address=("127.0.0.1", self.master_port))

    # Tests
    def test_03_connectionClosed(self):
        conn = self.getMasterConnection()
        self.app.listening_conn = object() # mark as running
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_08_askPartitionTable(self):
        node = self.app.nm.createStorage(
            address=("127.7.9.9", 1),
            uuid=self.getStorageUUID()
        )
        self.app.pt.setCell(1, node, CellStates.UP_TO_DATE)
        self.assertTrue(self.app.pt.hasOffset(1))
        conn = self.getMasterConnection()
        self.verification.askPartitionTable(conn)
        ptid, row_list = self.checkAnswerPartitionTable(conn, decode=True)
        self.assertEqual(len(row_list), 1009)

    def test_10_notifyPartitionChanges(self):
        # old partition change
        conn = self.getMasterConnection()
        self.verification.notifyPartitionChanges(conn, 1, ())
        self.verification.notifyPartitionChanges(conn, 0, ())
        self.assertEqual(self.app.pt.getID(), 1)

        # new node
        conn = self.getMasterConnection()
        new_uuid = self.getStorageUUID()
        cell = (0, new_uuid, CellStates.UP_TO_DATE)
        self.app.nm.createStorage(uuid=new_uuid)
        self.app.pt = PartitionTable(1, 1)
        self.app.dm = Mock({ })
        ptid = self.getPTID()
        # pt updated
        self.verification.notifyPartitionChanges(conn, ptid, (cell, ))
        # check db update
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].getParam(0), ptid)
        self.assertEqual(calls[0].getParam(1), (cell, ))

    def test_13_askUnfinishedTransactions(self):
        # client connection with no data
        self.app.dm = Mock({
            'getUnfinishedTIDList': [],
        })
        conn = self.getMasterConnection()
        self.verification.askUnfinishedTransactions(conn)
        (max_tid, tid_list) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 0)
        call_list = self.app.dm.mockGetNamedCalls('getUnfinishedTIDList')
        self.assertEqual(len(call_list), 1)
        call_list[0].checkArgs()

        # client connection with some data
        self.app.dm = Mock({
            'getUnfinishedTIDList': [p64(4)],
        })
        conn = self.getMasterConnection()
        self.verification.askUnfinishedTransactions(conn)
        (max_tid, tid_list) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 1)
        self.assertEqual(u64(tid_list[0]), 4)

    def test_14_askTransactionInformation(self):
        # ask from client conn with no data
        self.app.dm = Mock({
            'getTransaction': None,
        })
        conn = self.getMasterConnection()
        tid = p64(1)
        self.verification.askTransactionInformation(conn, tid)
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, ErrorCodes.TID_NOT_FOUND)
        call_list = self.app.dm.mockGetNamedCalls('getTransaction')
        self.assertEqual(len(call_list), 1)
        call_list[0].checkArgs(tid, all=True)

        # input some tmp data and ask from client, must find both transaction
        self.app.dm = Mock({
            'getTransaction': ([p64(2)], 'u2', 'd2', 'e2', False),
        })
        conn = self.getMasterConnection()
        self.verification.askTransactionInformation(conn, p64(1))
        tid, user, desc, ext, packed, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 1)
        self.assertEqual(user, 'u2')
        self.assertEqual(desc, 'd2')
        self.assertEqual(ext, 'e2')
        self.assertFalse(packed)
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 2)

    def test_15_askObjectPresent(self):
        # client connection with no data
        self.app.dm = Mock({
            'objectPresent': False,
        })
        conn = self.getMasterConnection()
        oid, tid = p64(1), p64(2)
        self.verification.askObjectPresent(conn, oid, tid)
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, ErrorCodes.OID_NOT_FOUND)
        call_list = self.app.dm.mockGetNamedCalls('objectPresent')
        self.assertEqual(len(call_list), 1)
        call_list[0].checkArgs(oid, tid)

        # client connection with some data
        self.app.dm = Mock({
            'objectPresent': True,
        })
        conn = self.getMasterConnection()
        self.verification.askObjectPresent(conn, oid, tid)
        oid, tid = self.checkAnswerObjectPresent(conn, decode=True)
        self.assertEqual(u64(tid), 2)
        self.assertEqual(u64(oid), 1)

    def test_16_deleteTransaction(self):
        # client connection with no data
        self.app.dm = Mock({
            'deleteTransaction': None,
        })
        conn = self.getMasterConnection()
        oid_list = [self.getOID(1), self.getOID(2)]
        tid = p64(1)
        self.verification.deleteTransaction(conn, tid, oid_list)
        call_list = self.app.dm.mockGetNamedCalls('deleteTransaction')
        self.assertEqual(len(call_list), 1)
        call_list[0].checkArgs(tid, oid_list)

    def test_17_commitTransaction(self):
        # commit a transaction
        conn = self.getMasterConnection()
        dm = Mock()
        self.app.dm = dm
        self.verification.commitTransaction(conn, p64(1))
        self.assertEqual(len(dm.mockGetNamedCalls("finishTransaction")), 1)
        call = dm.mockGetNamedCalls("finishTransaction")[0]
        tid = call.getParam(0)
        self.assertEqual(u64(tid), 1)

if __name__ == "__main__":
    unittest.main()

