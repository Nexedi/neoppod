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
from neo.tests import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.storage import StorageOperationHandler
from neo.lib.protocol import INVALID_PARTITION, Packets
from neo.lib.protocol import INVALID_TID, INVALID_OID

class StorageStorageHandlerTests(NeoUnitTestBase):

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
        # handler
        self.operation = StorageOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self.getFakeConnection()
        self.app.dm = Mock({'getNumPartitions': 1})
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
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEqual(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEqual(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = self.getFakeConnection()
        oid = self.getOID(1)
        tid = self.getNextTID()
        serial = self.getNextTID()
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEqual(len(self.app.event_queue), 0)
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(oid, serial, tid)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        oid = self.getOID(1)
        tid = self.getNextTID()
        serial = self.getNextTID()
        next_serial = self.getNextTID()
        H = "0" * 20
        # object found => answer
        self.app.dm = Mock({'getObject': (serial, next_serial, 0, H, '', None)})
        conn = self.getFakeConnection()
        self.assertEqual(len(self.app.event_queue), 0)
        self.operation.askObject(conn, oid=oid, serial=serial, tid=tid)
        self.assertEqual(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDsFrom(self):
        # well case => answer
        conn = self.getFakeConnection()
        self.app.dm = Mock({'getReplicationTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getPartitions': 1})
        tid = self.getNextTID()
        tid2 = self.getNextTID()
        self.operation.askTIDsFrom(conn, tid, tid2, 2, [1])
        calls = self.app.dm.mockGetNamedCalls('getReplicationTIDList')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid, tid2, 2, 1, 1)
        self.checkAnswerTidsFrom(conn)

    def test_26_askObjectHistoryFrom(self):
        min_oid = self.getOID(2)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = 4
        partition = 8
        num_partitions = 16
        tid = self.getNextTID()
        conn = self.getFakeConnection()
        self.app.dm = Mock({'getObjectHistoryFrom': {min_oid: [tid]},})
        self.app.pt = Mock({
            'getPartitions': num_partitions,
        })
        self.operation.askObjectHistoryFrom(conn, min_oid, min_serial,
            max_serial, length, partition)
        self.checkAnswerObjectHistoryFrom(conn)
        calls = self.app.dm.mockGetNamedCalls('getObjectHistoryFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(min_oid, min_serial, max_serial, length,
            num_partitions, partition)

    def test_askCheckTIDRange(self):
        count = 1
        tid_checksum = "1" * 20
        min_tid = self.getNextTID()
        num_partitions = 4
        length = 5
        partition = 6
        max_tid = self.getNextTID()
        self.app.dm = Mock({'checkTIDRange': (count, tid_checksum, max_tid)})
        self.app.pt = Mock({'getPartitions': num_partitions})
        conn = self.getFakeConnection()
        self.operation.askCheckTIDRange(conn, min_tid, max_tid, length, partition)
        calls = self.app.dm.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(min_tid, max_tid, length, num_partitions, partition)
        pmin_tid, plength, pcount, ptid_checksum, pmax_tid = \
            self.checkAnswerPacket(conn, Packets.AnswerCheckTIDRange,
            decode=True)
        self.assertEqual(min_tid, pmin_tid)
        self.assertEqual(length, plength)
        self.assertEqual(count, pcount)
        self.assertEqual(tid_checksum, ptid_checksum)
        self.assertEqual(max_tid, pmax_tid)

    def test_askCheckSerialRange(self):
        count = 1
        oid_checksum = "2" * 20
        min_oid = self.getOID(1)
        num_partitions = 4
        length = 5
        partition = 6
        serial_checksum = "3" * 20
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        max_oid = self.getOID(2)
        self.app.dm = Mock({'checkSerialRange': (count, oid_checksum, max_oid,
            serial_checksum, max_serial)})
        self.app.pt = Mock({'getPartitions': num_partitions})
        conn = self.getFakeConnection()
        self.operation.askCheckSerialRange(conn, min_oid, min_serial,
            max_serial, length, partition)
        calls = self.app.dm.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(min_oid, min_serial, max_serial, length,
            num_partitions, partition)
        pmin_oid, pmin_serial, plength, pcount, poid_checksum, pmax_oid, \
            pserial_checksum, pmax_serial = self.checkAnswerPacket(conn,
            Packets.AnswerCheckSerialRange, decode=True)
        self.assertEqual(min_oid, pmin_oid)
        self.assertEqual(min_serial, pmin_serial)
        self.assertEqual(length, plength)
        self.assertEqual(count, pcount)
        self.assertEqual(oid_checksum, poid_checksum)
        self.assertEqual(max_oid, pmax_oid)
        self.assertEqual(serial_checksum, pserial_checksum)
        self.assertEqual(max_serial, pmax_serial)

if __name__ == "__main__":
    unittest.main()
