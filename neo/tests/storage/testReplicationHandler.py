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
from neo.tests import NeoUnitTestBase
from neo.protocol import Packets, ZERO_OID, ZERO_TID
from neo.storage.handlers.replication import ReplicationHandler, add64
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
            rid=0, critical_tid=ZERO_TID):
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
            'getCurrentRID': rid,
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
            })
        return FakeApp

    def _checkReplicationStarted(self, conn, rid, replicator):
        min_tid, length, partition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(min_tid, ZERO_TID)
        self.assertEqual(length, RANGE_LENGTH)
        self.assertEqual(partition, rid)
        calls = replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(min_tid, length, partition)

    def _checkPacketTIDList(self, conn, tid_list):
        packet_list = [x.getParam(0) for x in conn.mockGetNamedCalls('ask')]
        self.assertEqual(len(packet_list), len(tid_list))
        for packet in packet_list:
            self.assertEqual(packet.getType(),
                Packets.AskTransactionInformation)
            ptid = packet.decode()[0]
            for tid in tid_list:
                if ptid == tid:
                    tid_list.remove(tid)
                    break
            else:
                raise AssertionFailed, '%s not found in %r' % (dump(ptid),
                    [dump(x) for x in tid_list])

    def _checkPacketSerialList(self, conn, object_list):
        packet_list = [x.getParam(0) for x in conn.mockGetNamedCalls('ask')]
        self.assertEqual(len(packet_list), len(object_list))
        for packet, (oid, serial) in zip(packet_list, object_list):
            self.assertEqual(packet.getType(),
                Packets.AskObject)
            self.assertEqual(packet.decode(), (oid, serial, None))

    def test_connectionLost(self):
        app = self.getApp()
        ReplicationHandler(app).connectionLost(None, None)
        self.assertEqual(len(app.replicator.mockGetNamedCalls('reset')), 1)

    def test_connectionFailed(self):
        app = self.getApp()
        ReplicationHandler(app).connectionFailed(None)
        self.assertEqual(len(app.replicator.mockGetNamedCalls('reset')), 1)

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
        tid_list = [self.getNextTID(), self.getNextTID()]
        app = self.getApp(conn=conn, tid_result=[])
        # With no known TID
        ReplicationHandler(app).answerTIDsFrom(conn, tid_list)
        self._checkPacketTIDList(conn, tid_list[:])
        # With first TID known
        conn = self.getFakeConnection()
        known_tid_list = [tid_list[0], ]
        unknown_tid_list = [tid_list[1], ]
        app = self.getApp(conn=conn, tid_result=known_tid_list)
        ReplicationHandler(app).answerTIDsFrom(conn, tid_list)
        self._checkPacketTIDList(conn, unknown_tid_list)

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
        oid_dict = FakeDict((
            (oid_1, [self.getNextTID(), self.getNextTID()]),
            (oid_2, [self.getNextTID()]),
            (oid_3, [self.getNextTID()]),
        ))
        flat_oid_list = []
        for oid, serial_list in oid_dict.iteritems():
            for serial in serial_list:
                flat_oid_list.append((oid, serial))
        app = self.getApp(conn=conn, history_result={})
        # With no known OID/Serial
        ReplicationHandler(app).answerObjectHistoryFrom(conn, oid_dict)
        self._checkPacketSerialList(conn, flat_oid_list)
        # With some known OID/Serials
        conn = self.getFakeConnection()
        app = self.getApp(conn=conn, history_result={
            oid_1: [oid_dict[oid_1][0], ],
            oid_3: [oid_dict[oid_3][0], ],
        })
        ReplicationHandler(app).answerObjectHistoryFrom(conn, oid_dict)
        self._checkPacketSerialList(conn, (
            (oid_1, oid_dict[oid_1][1]),
            (oid_2, oid_dict[oid_2][0]),
        ))

    def test_answerObject(self):
        conn = self.getFakeConnection()
        app = self.getApp(conn=conn)
        oid = self.getOID(1)
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        compression = 1
        checksum = 2
        data = 'foo'
        data_serial = None
        ReplicationHandler(app).answerObject(conn, oid, serial_start,
            serial_end, compression, checksum, data, data_serial)
        calls = app.dm.mockGetNamedCalls('storeTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(serial_start, [(oid, compression, checksum, data,
            data_serial)], None, False)

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
        pmin_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmin_tid, add64(max_tid, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, plength, ppartition)

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
        pmin_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmin_tid, add64(max_tid, 1))
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, plength, ppartition)

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
        pmin_oid, pmin_serial, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, ZERO_OID)
        self.assertEqual(pmin_serial, ZERO_TID)
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

    def test_answerCheckTIDRangeIdenticalChunkWithoutNext(self):
        min_tid = self.getNextTID()
        max_tid = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 1, 0, max_tid), rid=rid,
            conn=conn)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have: length, checksum and max_tid
        # match.
        handler.answerCheckTIDRange(conn, min_tid, length, length - 1, 0,
            max_tid)
        # Result: go on with object range checks
        pmin_oid, pmin_serial, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, ZERO_OID)
        self.assertEqual(pmin_serial, ZERO_TID)
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

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
        pmin_tid, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckTIDRange, decode=True)
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, plength, ppartition)

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
        self.assertEqual(len(calls), 2)
        tid_call, next_call = calls
        tid_packet = tid_call.getParam(0)
        next_packet = next_call.getParam(0)
        self.assertEqual(tid_packet.getType(), Packets.AskTIDsFrom)
        pmin_tid, pmax_tid, plength, ppartition = tid_packet.decode()
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(pmax_tid, critical_tid)
        self.assertEqual(plength, length)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('getTIDsFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition)
        self.assertEqual(next_packet.getType(), Packets.AskCheckTIDRange)
        pmin_tid, plength, ppartition = next_packet.decode()
        self.assertEqual(pmin_tid, add64(max_tid, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkTIDRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, plength, ppartition)

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
        tid_call, next_call = calls
        tid_packet = tid_call.getParam(0)
        next_packet = next_call.getParam(0)
        self.assertEqual(tid_packet.getType(), Packets.AskTIDsFrom)
        pmin_tid, pmax_tid, plength, ppartition = tid_packet.decode()
        self.assertEqual(pmin_tid, min_tid)
        self.assertEqual(pmax_tid, critical_tid)
        self.assertEqual(plength, length - 1)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('getTIDsFrom')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_tid, pmax_tid, plength, ppartition)
        self.assertEqual(next_packet.getType(), Packets.AskCheckSerialRange)
        pmin_oid, pmin_serial, plength, ppartition = next_packet.decode()
        self.assertEqual(pmin_oid, ZERO_OID)
        self.assertEqual(pmin_serial, ZERO_TID)
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

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
        pmin_oid, pmin_serial, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, max_oid)
        self.assertEqual(pmin_serial, add64(max_serial, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

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
        pmin_oid, pmin_serial, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, max_oid)
        self.assertEqual(pmin_serial, add64(max_serial, 1))
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

    def test_answerCheckSerialRangeIdenticalChunkWithoutNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        length = RANGE_LENGTH / 2
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(serial_check_result=(length - 1, 0, max_oid, 1,
            max_serial), rid=rid, conn=conn)
        handler = ReplicationHandler(app)
        # Peer has the same data as we have
        handler.answerCheckSerialRange(conn, min_oid, min_serial, length,
            length - 1, 0, max_oid, 1, max_serial)
        # Result: mark replication as done
        self.checkNoPacketSent(conn)
        self.assertTrue(app.replicator.replication_done)

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
        pmin_oid, pmin_serial, plength, ppartition = self.checkAskPacket(conn,
            Packets.AskCheckSerialRange, decode=True)
        self.assertEqual(pmin_oid, min_oid)
        self.assertEqual(pmin_serial, min_serial)
        self.assertEqual(plength, length / 2)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

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
        self.assertEqual(len(calls), 2)
        serial_call, next_call = calls
        serial_packet = serial_call.getParam(0)
        next_packet = next_call.getParam(0)
        self.assertEqual(serial_packet.getType(), Packets.AskObjectHistoryFrom)
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
        self.assertEqual(next_packet.getType(), Packets.AskCheckSerialRange)
        pmin_oid, pmin_serial, plength, ppartition = next_packet.decode()
        self.assertEqual(pmin_oid, max_oid)
        self.assertEqual(pmin_serial, add64(max_serial, 1))
        self.assertEqual(plength, RANGE_LENGTH)
        self.assertEqual(ppartition, rid)
        calls = app.replicator.mockGetNamedCalls('checkSerialRange')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(pmin_oid, pmin_serial, plength, ppartition)

    def test_answerCheckSerialRangeDifferentSmallChunkWithoutNext(self):
        min_oid = self.getOID(1)
        max_oid = self.getOID(10)
        min_serial = self.getNextTID()
        max_serial = self.getNextTID()
        critical_tid = self.getNextTID()
        length = MIN_RANGE_LENGTH - 1
        rid = 12
        conn = self.getFakeConnection()
        app = self.getApp(tid_check_result=(length - 5, 0, max_oid,
            1, max_serial), rid=rid, conn=conn, critical_tid=critical_tid)
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

if __name__ == "__main__":
    unittest.main()
