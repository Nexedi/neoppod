#
# Copyright (C) 2010  Nexedi SA
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
from neo.lib.util import add64
from neo.tests import NeoUnitTestBase
from neo.lib.protocol import Packets, ZERO_OID, ZERO_TID
from neo.storage.handlers.replication import ReplicationHandler
from neo.storage.handlers.replication import RANGE_LENGTH, MIN_RANGE_LENGTH

class FakeDict(object):
    def __init__(self, items):
        self._items = items
        self._dict = dict(items)
        assert len(self._dict) == len(items), self._dict

    def iteritems(self):
        for item in self._items:
            yield item

    def iterkeys(self):
        for key, value in self.iteritems():
            yield key

    def itervalues(self):
        for key, value in self.iteritems():
            yield value

    def items(self):
        return self._items[:]

    def keys(self):
        return [x for x, y in self._items]

    def values(self):
        return [y for x, y in self._items]

    def __getitem__(self, key):
        return self._dict[key]

    def __getattr__(self, key):
        return getattr(self._dict, key)

    def __len__(self):
        return len(self._dict)

class StorageReplicationHandlerTests(NeoUnitTestBase):

    def setup(self):
        pass

    def teardown(self):
        pass

    def getApp(self, conn=None, tid_check_result=(0, 0, ZERO_TID),
                serial_check_result=(0, 0, ZERO_OID, 0, ZERO_TID),
                tid_result=(),
                history_result=None,
                rid=0, critical_tid=ZERO_TID,
                num_partitions=1,
            ):
        if history_result is None:
            history_result = {}
        replicator = Mock({
            '__repr__': 'Fake replicator',
            'reset': None,
            'checkSerialRange': None,
            'checkTIDRange': None,
            'getTIDCheckResult': tid_check_result,
            'getSerialCheckResult': serial_check_result,
            'getTIDsFromResult': tid_result,
            'getObjectHistoryFromResult': history_result,
            'checkSerialRange': None,
            'checkTIDRange': None,
            'getTIDsFrom': None,
            'getObjectHistoryFrom': None,
            'getCurrentOffset': rid,
            'getCurrentCriticalTID': critical_tid,
        })
        def isCurrentConnection(other_conn):
            return other_conn is conn
        replicator.isCurrentConnection = isCurrentConnection
        real_replicator = replicator
        class FakeApp(object):
            replicator = real_replicator
            dm = Mock({
                'storeTransaction': None,
                'deleteObject': None,
            })
            pt = Mock({
                'getPartitions': num_partitions,
            })
        return FakeApp

    def _checkReplicationStarted(self, conn, rid, replicator):
        min_tid, max_tid, length, partition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(min_tid, ZERO_TID)
        self.assertEqual(length, RANGE_LENGTH)
        self.assertEqual(partition, rid)
        calls = replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(min_tid, max_tid, length, partition)

    def _checkPacketTIDList(self, conn, tid_list, next_tid, app):
        packet_list = [x.getParam(0) for x in conn.mockGetNamedCalls('ask')]
        packet_list, next_range = packet_list[:-1], packet_list[-1]

        self.assertEqual(type(next_range), Packets.AskCheckTIDRange)
        pmin_tid, plength, ppartition = next_range.decode()
        self.assertEqual(pmin_tid, add64(next_tid, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, app.replicator.getCurrentOffset())
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, plength, ppartition)

        self.assertEqual(len(packet_list), len(tid_list))
        for packet in packet_list:
            self.assertEqual(type(packet),
                Packets.AskTransactionInformation)
            ptid = packet.decode()[0]
            for tid in tid_list:
                if ptid == tid:
                    tid_list.remove(tid)
                    break
            else:
                raise AssertionFailed, '%s not found in %r' % (dump(ptid),
                    [dump(x) for x in tid_list])

    def _checkPacketSerialList(self, conn, object_list, next_oid, next_serial, app):
        packet_list = [x.getParam(0) for x in conn.mockGetNamedCalls('ask')]
        packet_list, next_range = packet_list[:-1], packet_list[-1]

        self.assertEqual(type(next_range), Packets.AskCheckSerialRange)
        pmin_oid, pmin_serial, plength, ppartition = next_range.decode()
        self.assertEqual(pmin_oid, next_oid)
        self.assertEqual(pmin_serial, add64(next_serial, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, app.replicator.getCurrentOffset())
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

        self.assertEqual(len(packet_list), len(object_list),
            ([x.decode() for x in packet_list], object_list))
        reference_set = set((x + (None, ) for x in object_list))
        packet_set = set((x.decode() for x in packet_list))
        assert len(packet_list) == len(reference_set) == len(packet_set)
        self.assertEqual(reference_set, packet_set)

    def test_connectionLost(self):
        app = self.getApp()
        ReplicationHandler(app).connectionLost(None, None)
        self.assertEqual(len(app.replicator.mockGetNamedCalls('storageLost')), 1)

    def test_connectionFailed(self):
        app = self.getApp()
        ReplicationHandler(app).connectionFailed(None)
        self.assertEqual(len(app.replicator.mockGetNamedCalls('storageLost')), 1)

    def test_acceptIdentification(self):
        rid = 24
        app = self.getApp(rid=rid)
        conn = self.getFakeConnection()
        replication = ReplicationHandler(app)
        replication.acceptIdentification(conn, None, None, None,
            None, None)
        self._checkReplicationStarted(conn, rid, app.replicator)

    def test_startReplication(self):
        rid = 24
        app = self.getApp(rid=rid)
        conn = self.getFakeConnection()
        ReplicationHandler(app).startReplication(conn)
        self._checkReplicationStarted(conn, rid, app.replicator)

    def test_answerTIDsFrom(self):
        conn = self.getFakeConnection()
        tid_list = [self.getOID(0), self.getOID(1), self.getOID(2)]
        app = self.getApp(conn=conn, tid_result=[])
        # With no known TID
        ReplicationHandler(app).answerTIDsFrom(conn, tid_list)
        # With some TIDs known
        conn = self.getFakeConnection()
        known_tid_list = [tid_list[0], tid_list[1]]
        unknown_tid_list = [tid_list[2], ]
        app = self.getApp(conn=conn, tid_result=known_tid_list)
        ReplicationHandler(app).answerTIDsFrom(conn, tid_list[1:])
        calls = app.dm.mockGetNamedCalls('deleteTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid_list[0])

    def test_answerTransactionInformation(self):
        conn = self.getFakeConnection()
        app = self.getApp(conn=conn)
        tid = self.getNextTID()
        user = 'foo'
        desc = 'bar'
        ext = 'baz'
        packed = True
        oid_list = [self.getOID(1), self.getOID(2)]
        ReplicationHandler(app).answerTransactionInformation(conn, tid, user,
            desc, ext, packed, oid_list)
        calls = app.dm.mockGetNamedCalls('storeTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid, (), (oid_list, user, desc, ext, packed), False)

    def test_answerObjectHistoryFrom(self):
        conn = self.getFakeConnection()
        oid_1 = self.getOID(1)
        oid_2 = self.getOID(2)
        oid_3 = self.getOID(3)
        oid_4 = self.getOID(4)
        oid_5 = self.getOID(5)
        tid_list = [self.getOID(x) for x in xrange(7)]
        oid_dict = FakeDict((
            (oid_1, [tid_list[0], tid_list[1]]),
            (oid_2, [tid_list[2], tid_list[3]]),
            (oid_4, [tid_list[5]]),
        ))
        flat_oid_list = []
        for oid, serial_list in oid_dict.iteritems():
            for serial in serial_list:
                flat_oid_list.append((oid, serial))
        app = self.getApp(conn=conn, history_result={})
        # With no known OID/Serial
        ReplicationHandler(app).answerObjectHistoryFrom(conn, oid_dict)
        # With some known OID/Serials
        # For test to be realist, history_result should contain the same
        # number of serials as oid_dict, otherise it just tests the special
        # case of the last check in a partition.
        conn = self.getFakeConnection()
        app = self.getApp(conn=conn, history_result={
            oid_1: [oid_dict[oid_1][0], ],
            oid_3: [tid_list[2]],
            oid_4: [tid_list[4], oid_dict[oid_4][0], tid_list[6]],
            oid_5: [tid_list[6]],
        })
        ReplicationHandler(app).answerObjectHistoryFrom(conn, oid_dict)
        calls = app.dm.mockGetNamedCalls('deleteObject')
        actual_deletes = set(((x.getParam(0), x.getParam(1)) for x in calls))
        expected_deletes = set((
            (oid_3, tid_list[2]),
            (oid_4, tid_list[4]),
        ))
        self.assertEqual(actual_deletes, expected_deletes)

    def test_answerObject(self):
        conn = self.getFakeConnection()
        app = self.getApp(conn=conn)
        oid = self.getOID(1)
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        compression = 1
        checksum = "0" * 20
        data = 'foo'
        data_serial = None
        ReplicationHandler(app).answerObject(conn, oid, serial_start,
            serial_end, compression, checksum, data, data_serial)
        calls = app.dm.mockGetNamedCalls('storeTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(serial_start, [(oid, checksum, data_serial)],
            None, False)

    # CheckTIDRange
    def test_answerCheckTIDFullRangeIdenticalChunkWithNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        critical_tid = self.getNextTID()
        assert max_tid < critical_tid
        length = RANGE_LENGTH
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have: length, checksum and max_tid
        # match.
        handler.answerCheckTIDRange(conn, min_tid, length, length, 0, max_tid)
        # Result: go on with next chunk
        pmin_tid, pmax_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmin_tid, add64(max_tid, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition)

    def test_answerCheckTIDSmallRangeIdenticalChunkWithNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        critical_tid = self.getNextTID()
        assert max_tid < critical_tid
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have: length, checksum and max_tid
        # match.
        handler.answerCheckTIDRange(conn, min_tid, length, length, 0, max_tid)
        # Result: go on with next chunk
        pmin_tid, pmax_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmax_tid, critical_tid)
        self.assertEqual(pmin_tid, add64(max_tid, 1))
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition)

    def test_answerCheckTIDRangeIdenticalChunkAboveCriticalTID(self):
        critical_tid = self.getNextTID()
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        assert critical_tid < max_tid
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have: length, checksum and max_tid
        # match.
        handler.answerCheckTIDRange(conn, min_tid, length, length, 0, max_tid)
        # Result: go on with object range checks
        pmin_oid, pmin_serial, pmax_tid, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, ZERO_OID)
        self.assertEqual(pmin_serial, ZERO_TID)
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_tid, plength, ppartition)

    def test_answerCheckTIDRangeIdenticalChunkWithoutNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        num_partitions = 13
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 1, 0, max_tid), rid=rid,
            conn=conn, num_partitions=num_partitions)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have: length, checksum and max_tid
        # match.
        handler.answerCheckTIDRange(conn, min_tid, length, length - 1, 0,
            max_tid)
        # Result: go on with object range checks
        pmin_oid, pmin_serial, pmax_tid, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, ZERO_OID)
        self.assertEqual(pmin_serial, ZERO_TID)
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_tid, plength, ppartition)
        # ...and delete partition tail
        calls = app.dm.mockGetNamedCalls('deleteTransactionsAbove')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(num_partitions, rid, add64(max_tid, 1), ZERO_TID)

    def test_answerCheckTIDRangeDifferentBigChunk(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        critical_tid = self.getNextTID()
        assert min_tid < max_tid < critical_tid, (min_tid, max_tid,
            critical_tid)
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has different data
        handler.answerCheckTIDRange(conn, min_tid, length, length, 0, max_tid)
        # Result: ask again, length halved
        pmin_tid, pmax_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition)

    def test_answerCheckTIDRangeDifferentSmallChunkWithNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        critical_tid = self.getNextTID()
        length = MIN_RANGE_LENGTH - 1
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has different data
        handler.answerCheckTIDRange(conn, min_tid, length, length, 0, max_tid)
        # Result: ask tid list, and ask next chunk
        calls = conn.mockGetNamedCalls('ask')
        self.assertEqual(len(calls), 1)
        tid_packet = calls[0].getParam(0)
        self.assertEqual(type(tid_packet), Packets.AskTIDsFrom)
        pmin_tid, pmax_tid, plength, ppartition = tid_packet.decode()
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(pmax_tid, critical_tid)
        self.assertEqual(plength, length)
        self.assertEqual(ppartition, [rid])
        calls = app.replicator.mockGetNamedCalls('getTIDsFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition[0])

    def test_answerCheckTIDRangeDifferentSmallChunkWithoutNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        critical_tid = self.getNextTID()
        length = MIN_RANGE_LENGTH - 1
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_tid), rid=rid,
            conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has different data, and less than length
        handler.answerCheckTIDRange(conn, min_tid, length, length - 1, 0,
            max_tid)
        # Result: ask tid list, and start replicating object range
        calls = conn.mockGetNamedCalls('ask')
        self.assertEqual(len(calls), 2)
        tid_packet = calls[0].getParam(0)
        self.assertEqual(type(tid_packet), Packets.AskTIDsFrom)
        pmin_tid, pmax_tid, plength, ppartition = tid_packet.decode()
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(pmax_tid, critical_tid)
        self.assertEqual(plength, length - 1)
        self.assertEqual(ppartition, [rid])
        calls = app.replicator.mockGetNamedCalls('getTIDsFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition[0])

    # CheckSerialRange
    def test_answerCheckSerialFullRangeIdenticalChunkWithNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = RANGE_LENGTH
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(serial_check_result=(length, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length, 0, max_oid, 1, max_serial)
        # Result: go on with next chunk
        pmin_oid, pmin_serial, pmax_tid, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, max_oid)
        self.assertEqual(pmin_serial, add64(max_serial, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_tid, plength, ppartition)

    def test_answerCheckSerialSmallRangeIdenticalChunkWithNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(serial_check_result=(length, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length, 0, max_oid, 1, max_serial)
        # Result: go on with next chunk
        pmin_oid, pmin_serial, pmax_tid, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, max_oid)
        self.assertEqual(pmin_serial, add64(max_serial, 1))
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_tid, plength, ppartition)

    def test_answerCheckSerialRangeIdenticalChunkWithoutNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        num_partitions = 13
        conn = self.getFakeConnection()
        app = self.getApp(serial_check_result=(length - 1, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn, num_partitions=num_partitions)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length - 1, 0, max_oid, 1, max_serial)
        # Result: mark replication as done
        self.checkNoPacketSent(conn)
        self.assertTrue(app.replicator.replication_done)
        # ...and delete partition tail
        calls = app.dm.mockGetNamedCalls('deleteObjectsAbove')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(num_partitions, rid, max_oid, add64(max_serial, 1),
            ZERO_TID)

    def test_answerCheckSerialRangeDifferentBigChunk(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn)
        handler = ReplicationHandler(app)
        # Peer has different data
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length, 0, max_oid, 1, max_serial)
        # Result: ask again, length halved
        pmin_oid, pmin_serial, pmax_tid, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, min_oid)
        self.assertEqual(pmin_serial, min_serial)
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_tid, plength, ppartition)

    def test_answerCheckSerialRangeDifferentSmallChunkWithNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        critical_tid = self.getNextTID()
        length = MIN_RANGE_LENGTH - 1
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn, critical_tid=critical_tid)
        handler = ReplicationHandler(app)
        # Peer has different data
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length, 0, max_oid, 1, max_serial)
        # Result: ask serial list, and ask next chunk
        calls = conn.mockGetNamedCalls('ask')
        self.assertEqual(len(calls), 1)
        serial_packet = calls[0].getParam(0)
        self.assertEqual(type(serial_packet), Packets.AskObjectHistoryFrom)
        pmin_oid, pmin_serial, pmax_serial, plength, ppartition = \
            serial_packet.decode()
        self.assertEqual(pmin_oid, min_oid)
        self.assertEqual(pmin_serial, min_serial)
        self.assertEqual(pmax_serial, critical_tid)
        self.assertEqual(plength, length)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('getObjectHistoryFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_serial, plength,
            ppartition)

    def test_answerCheckSerialRangeDifferentSmallChunkWithoutNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        critical_tid = self.getNextTID()
        length = MIN_RANGE_LENGTH - 1
        rid = 12
        num_partitions = 13
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_oid,
            1, max_serial), rid=rid, conn=conn, critical_tid=critical_tid,
            num_partitions=num_partitions,
        )
        handler = ReplicationHandler(app)
        # Peer has different data, and less than length
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length - 1, 0, max_oid, 1, max_serial)
        # Result: ask tid list, and mark replication as done
        pmin_oid, pmin_serial, pmax_serial, plength, ppartition = \
            self.checkAskPacket(conn, Packets.AskObjectHistoryFrom,
            decode=True)
        self.assertEqual(pmin_oid, min_oid)
        self.assertEqual(pmin_serial, min_serial)
        self.assertEqual(pmax_serial, critical_tid)
        self.assertEqual(plength, length - 1)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('getObjectHistoryFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, pmax_serial, plength,
            ppartition)
        self.assertTrue(app.replicator.replication_done)
        # ...and delete partition tail
        calls = app.dm.mockGetNamedCalls('deleteObjectsAbove')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(num_partitions, rid, max_oid, add64(max_serial, 1),
            critical_tid)

if __name__ == "__main__":
    unittest.main()
