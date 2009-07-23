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
from mock import Mock
from neo.tests import NeoTestBase
from neo import protocol
from neo.node import MasterNode
from neo.pt import PartitionTable
from neo.storage.app import Application, StorageNode
from neo.storage.handlers.verification import VerificationHandler
from neo.protocol import STORAGE_NODE_TYPE, MASTER_NODE_TYPE, CLIENT_NODE_TYPE
from neo.protocol import BROKEN_STATE, RUNNING_STATE, Packet, INVALID_UUID, \
     UP_TO_DATE_STATE, INVALID_OID, INVALID_TID, PROTOCOL_ERROR_CODE
from neo.protocol import ACCEPT_NODE_IDENTIFICATION, REQUEST_NODE_IDENTIFICATION, \
     NOTIFY_PARTITION_CHANGES, STOP_OPERATION, ASK_LAST_IDS, ASK_PARTITION_TABLE, \
     ANSWER_OBJECT_PRESENT, ASK_OBJECT_PRESENT, OID_NOT_FOUND_CODE, LOCK_INFORMATION, \
     UNLOCK_INFORMATION, TID_NOT_FOUND_CODE, ASK_TRANSACTION_INFORMATION, \
     COMMIT_TRANSACTION, ASK_UNFINISHED_TRANSACTIONS, SEND_PARTITION_TABLE
from neo.protocol import ERROR, BROKEN_NODE_DISALLOWED_CODE, ASK_PRIMARY_MASTER
from neo.protocol import ANSWER_PRIMARY_MASTER
from neo.exception import PrimaryFailure, OperationFailure
from neo.storage.mysqldb import MySQLDatabaseManager, p64, u64

class StorageVerificationHandlerTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.ERROR)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getConfigFile(master_number=1)
        self.app = Application(config, "storage1")
        self.verification = VerificationHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.client_port = 11011
        self.num_partitions = 1009
        self.num_replicas = 2
        self.app.num_partitions = 1009
        self.app.num_replicas = 2
        self.app.operational = False
        self.app.load_lock_dict = {}
        self.app.pt = PartitionTable(self.app.num_partitions, self.app.num_replicas)

        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    # Tests
    def test_02_timeoutExpired(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.timeoutExpired, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_03_connectionClosed(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_04_peerBroken(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.peerBroken, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_07_handleAskLastIDs(self):
        uuid = self.getNewUUID()
        packet = Mock()
        # return invalid if db store nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.verification.handleAskLastIDs(conn, packet)
        oid, tid, ptid = self.checkAnswerLastIDs(conn, decode=True)
        self.assertEqual(oid, INVALID_OID)
        self.assertEqual(tid, INVALID_TID)
        self.assertEqual(ptid, self.app.ptid)

        # return value stored in db
        # insert some oid
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.app.dm.begin()
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (3, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (1, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (2, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into tobj (oid, serial, compression,
        checksum, value) values (5, 'A', 0, 0, '')""")
        # insert some tid
        self.app.dm.query("""insert into trans (tid, oids, user,
                description, ext) values (1, '', '', '', '')""")
        self.app.dm.query("""insert into trans (tid, oids, user,
                description, ext) values (2, '', '', '', '')""")
        self.app.dm.query("""insert into ttrans (tid, oids, user,
                description, ext) values (3, '', '', '', '')""")
        # max tid is in tobj (serial)
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        self.app.dm.commit()
        self.verification.handleAskLastIDs(conn, packet)
        self.checkAnswerLastIDs(conn)
        oid, tid, ptid = self.checkAnswerLastIDs(conn, decode=True)
        self.assertEqual(u64(oid), 5)
        self.assertEqual(u64(tid), 4)
        self.assertEqual(ptid, self.app.ptid)
        
    def test_08_handleAskPartitionTable(self):
        uuid = self.getNewUUID()
        packet = Mock()
        # try to get unknown offset
        self.assertEqual(len(self.app.pt.getNodeList()), 0)
        self.assertFalse(self.app.pt.hasOffset(1))
        self.assertEqual(len(self.app.pt.getCellList(1)), 0)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.verification.handleAskPartitionTable(conn, packet, [1,])
        ptid, row_list = self.checkAnswerPartitionTable(conn, decode=True)
        self.assertEqual(len(row_list), 1)
        offset, rows = row_list[0]
        self.assertEqual(offset, 1)
        self.assertEqual(len(rows), 0)

        # try to get known offset
        node = StorageNode(("127.7.9.9", 1), self.getNewUUID())
        self.app.pt.setCell(1, node, UP_TO_DATE_STATE)
        self.assertTrue(self.app.pt.hasOffset(1))
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.verification.handleAskPartitionTable(conn, packet, [1,])
        ptid, row_list = self.checkAnswerPartitionTable(conn, decode=True)
        self.assertEqual(len(row_list), 1)
        offset, rows = row_list[0]
        self.assertEqual(offset, 1)
        self.assertEqual(len(rows), 1)

    def test_10_handleNotifyPartitionChanges(self):
        # old partition change
        conn = Mock({
            "isServerConnection": False,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        packet = Packet(msg_type=NOTIFY_PARTITION_CHANGES)
        self.app.ptid = 1
        self.verification.handleNotifyPartitionChanges(conn, packet, 0, ())
        self.assertEquals(self.app.ptid, 1)

        # new node
        conn = Mock({
            "isServerConnection": False,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        packet = Packet(msg_type=NOTIFY_PARTITION_CHANGES)
        cell = (0, self.getNewUUID(), UP_TO_DATE_STATE)
        count = len(self.app.nm.getNodeList())
        self.app.pt = PartitionTable(1, 1)
        self.app.dm = Mock({ })
        ptid, self.ptid = self.getTwoIDs()
        # pt updated
        self.verification.handleNotifyPartitionChanges(conn, packet, ptid, (cell, ))
        self.assertEquals(len(self.app.nm.getNodeList()), count + 1)
        # check db update
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), ptid)
        self.assertEquals(calls[0].getParam(1), (cell, ))

    def test_11_handleStartOperation(self):
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        self.assertFalse(self.app.operational)
        packet = Packet(msg_type=STOP_OPERATION)
        self.verification.handleStartOperation(conn, packet)
        self.assertTrue(self.app.operational)

    def test_12_handleStopOperation(self):
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        packet = Packet(msg_type=STOP_OPERATION)
        self.assertRaises(OperationFailure, self.verification.handleStopOperation, conn, packet)

    def test_13_handleAskUnfinishedTransactions(self):
        # client connection with no data
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False})
        packet = Packet(msg_type=ASK_UNFINISHED_TRANSACTIONS)
        self.verification.handleAskUnfinishedTransactions(conn, packet)
        (tid_list, ) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 0)

        # client connection with some data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        self.app.dm.commit()
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False})
        packet = Packet(msg_type=ASK_UNFINISHED_TRANSACTIONS)
        self.verification.handleAskUnfinishedTransactions(conn, packet)
        (tid_list, ) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 1)
        self.assertEqual(u64(tid_list[0]), 4)

    def test_14_handleAskTransactionInformation(self):
        # ask from client conn with no data
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.verification.handleAskTransactionInformation(conn, packet, p64(1))
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, TID_NOT_FOUND_CODE)

        # input some tmp data and ask from client, must find both transaction
        self.app.dm.begin()
        self.app.dm.query("""insert into ttrans (tid, oids, user,
        description, ext) values (3, '%s', 'u1', 'd1', 'e1')""" %(p64(4),))
        self.app.dm.query("""insert into trans (tid, oids, user,
        description, ext) values (1,'%s', 'u2', 'd2', 'e2')""" %(p64(2),)) 
        self.app.dm.commit()
        # object from trans
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.verification.handleAskTransactionInformation(conn, packet, p64(1))
        tid, user, desc, ext, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 1)
        self.assertEqual(user, 'u2')
        self.assertEqual(desc, 'd2')
        self.assertEqual(ext, 'e2')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 2)
        # object from ttrans
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.verification.handleAskTransactionInformation(conn, packet, p64(3))
        tid, user, desc, ext, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 3)
        self.assertEqual(user, 'u1')
        self.assertEqual(desc, 'd1')
        self.assertEqual(ext, 'e1')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 4)

        # input some tmp data and ask from server, must find one transaction
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': True })
        # find the one in trans
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.verification.handleAskTransactionInformation(conn, packet, p64(1))
        tid, user, desc, ext, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 1)
        self.assertEqual(user, 'u2')
        self.assertEqual(desc, 'd2')
        self.assertEqual(ext, 'e2')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 2)
        # do not find the one in ttrans
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': True })
        packet = Packet(msg_type=ASK_TRANSACTION_INFORMATION)
        self.verification.handleAskTransactionInformation(conn, packet, p64(3))
        code, message = self.checkErrorPacket(conn, decode=True)     
        self.assertEqual(code, TID_NOT_FOUND_CODE)

    def test_15_handleAskObjectPresent(self):
        # client connection with no data
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False})
        packet = Packet(msg_type=ASK_OBJECT_PRESENT)
        self.verification.handleAskObjectPresent(conn, packet, p64(1), p64(2))
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, OID_NOT_FOUND_CODE)

        # client connection with some data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (1, 2, 0, 0, '')""")
        self.app.dm.commit()
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False})
        packet = Packet(msg_type=ASK_OBJECT_PRESENT)
        self.verification.handleAskObjectPresent(conn, packet, p64(1), p64(2))
        oid, tid = self.checkAnswerObjectPresent(conn, decode=True)
        self.assertEqual(u64(tid), 2)
        self.assertEqual(u64(oid), 1)

    def test_16_handleDeleteTransaction(self):
        # client connection with no data
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False})
        packet = Packet(msg_type=ASK_OBJECT_PRESENT)
        self.verification.handleDeleteTransaction(conn, packet, p64(1))
        # client connection with data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (1, 2, 0, 0, '')""")
        self.app.dm.commit()
        self.verification.handleDeleteTransaction(conn, packet, p64(2))
        result = self.app.dm.query('select * from tobj')
        self.assertEquals(len(result), 0)

    def test_17_handleCommitTransaction(self):
        # commit a transaction
        conn = Mock({ "getAddress" : ("127.0.0.1", self.master_port),
                      'isServerConnection': False })
        dm = Mock()
        self.app.dm = dm
        packet = Packet(msg_type=COMMIT_TRANSACTION)
        self.verification.handleCommitTransaction(conn, packet, p64(1))
        self.assertEqual(len(dm.mockGetNamedCalls("finishTransaction")), 1)
        call = dm.mockGetNamedCalls("finishTransaction")[0]
        tid = call.getParam(0)
        self.assertEqual(u64(tid), 1)

if __name__ == "__main__":
    unittest.main()

