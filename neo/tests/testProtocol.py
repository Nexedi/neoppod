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

import unittest, os
from mock import Mock
from neo.protocol import *
from time import time, gmtime

class testProtocol(unittest.TestCase):

    def setUp(self):
        self.ltid = INVALID_TID

    def tearDown(self):
        pass

    def getNextTID(self):
        tm = time()
        gmt = gmtime(tm)
        upper = ((((gmt.tm_year - 1900) * 12 + gmt.tm_mon - 1) * 31 \
                  + gmt.tm_mday - 1) * 24 + gmt.tm_hour) * 60 + gmt.tm_min
        lower = int((gmt.tm_sec % 60 + (tm - int(tm))) / (60.0 / 65536.0 / 65536.0))
        tid = pack('!LL', upper, lower)
        if tid <= self.ltid:
            upper, lower = unpack('!LL', self.ltid)
            if lower == 0xffffffff:
                # This should not happen usually.
                from datetime import timedelta, datetime
                d = datetime(gmt.tm_year, gmt.tm_mon, gmt.tm_mday,
                             gmt.tm_hour, gmt.tm_min) \
                        + timedelta(0, 60)
                upper = ((((d.year - 1900) * 12 + d.month - 1) * 31 \
                          + d.day - 1) * 24 + d.hour) * 60 + d.minute
                lower = 0
            else:
                lower += 1
            tid = pack('!LL', upper, lower)
        self.ltid = tid
        return tid

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def test_01_Packet_init(self):
        p = Packet(msg_id=1, msg_type=ASK_PRIMARY_MASTER, body=None)
        self.assertEqual(p._id, 1)
        self.assertEqual(p._type, ASK_PRIMARY_MASTER)
        self.assertEqual(p._body, None)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_PRIMARY_MASTER)
        self.assertEqual(len(p), PACKET_HEADER_SIZE)

    def test_02_error(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.error(1, 10, "error message")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, 10)
        self.assertEqual(error_len, len("error_message"))
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "error message")

    def test_03_protocolError(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.protocolError(1, "bad protocol")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, PROTOCOL_ERROR_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "protocol error: bad protocol")

    def test_04_internalError(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.internalError(1, "bad internal")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, INTERNAL_ERROR_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "internal error: bad internal")

    def test_05_notReady(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.notReady(1, "wait")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, NOT_READY_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "not ready: wait")

    def test_06_brokenNodeDisallowedError(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.brokenNodeDisallowedError(1, "broken")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, BROKEN_NODE_DISALLOWED_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "broken node disallowed error: broken")

    def test_07_oidNotFound(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.oidNotFound(1, "no oid")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, OID_NOT_FOUND_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "oid not found: no oid")

    def test_08_oidNotFound(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.tidNotFound(1, "no tid")
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ERROR)
        error_code, error_len = unpack('!HL', p._body[:6])
        self.assertEqual(error_code, TID_NOT_FOUND_CODE)
        error_message = p._body[6:6+error_len]
        self.assertEqual(error_message, "tid not found: no tid")

    def test_09_ping(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.ping(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), PING)
        self.assertEqual(p._body, '')

    def test_10_pong(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.pong(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), PONG)
        self.assertEqual(p._body, '')

    def test_11_requestNodeIdentification(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        uuid = self.getNewUUID()
        p.requestNodeIdentification(1,
                                    CLIENT_NODE_TYPE,
                                    uuid,
                                    "127.0.0.1",
                                    9080,
                                    "unittest"
                                    )
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), REQUEST_NODE_IDENTIFICATION)
        protocol_version1, protocol_version2,node, p_uuid, ip, port, name_len  = unpack('!LLH16s4sHL', p._body[:-len("unittest")])
        self.assertEqual(protocol_version1, PROTOCOL_VERSION[0])
        self.assertEqual(protocol_version2, PROTOCOL_VERSION[1])
        self.assertEqual(node, CLIENT_NODE_TYPE)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, inet_aton("127.0.0.1"))
        self.assertEqual(port, 9080)
        self.assertEqual(name_len, len("unittest"))
        name = p._body[-name_len:]
        self.assertEqual(name, "unittest")

    def test_12_acceptNodeIdentification(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        uuid = self.getNewUUID()
        p.acceptNodeIdentification(1,
                                   CLIENT_NODE_TYPE,
                                   uuid,
                                   "127.0.0.1",
                                   9080,
                                   10,
                                   20
                                   )
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ACCEPT_NODE_IDENTIFICATION)
        node, p_uuid, ip, port, nb_partitions, nb_replicas  = unpack('!H16s4sHLL', p._body)
        self.assertEqual(node, CLIENT_NODE_TYPE)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, inet_aton("127.0.0.1"))
        self.assertEqual(port, 9080)
        self.assertEqual(nb_partitions, 10)
        self.assertEqual(nb_replicas, 20)

    def test_13_askPrimaryMaster(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askPrimaryMaster(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_PRIMARY_MASTER)
        self.assertEqual(p._body, '')

    def test_14_answerPrimaryMaster(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        master_list = [("127.0.0.1", 1, uuid1),
                       ("127.0.0.2", 2, uuid2),
                       ("127.0.0.3", 3, uuid3)]
        p.answerPrimaryMaster(1,
                              uuid,
                              master_list
                              )
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_PRIMARY_MASTER)
        primary_uuid, len_master_list  = unpack('!16sL', p._body[:20])
        self.assertEqual(primary_uuid, uuid)
        self.assertEqual(len_master_list, len(master_list))
        for x in xrange(len_master_list):
            ip_address, port, uuid = unpack('!4sH16s', p._body[20+x*22:42+x*22])
            ip_address = inet_ntoa(ip_address)
            node = (ip_address, port, uuid)
            self.failUnless(node in master_list)
            master_list.remove(node)
        self.assertEqual(len(master_list), 0)

    def test_15_announcePrimaryMaster(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.announcePrimaryMaster(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANNOUNCE_PRIMARY_MASTER)
        self.assertEqual(p._body, '')

    def test_16_reelectPrimaryMaster(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.reelectPrimaryMaster(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), REELECT_PRIMARY_MASTER)
        self.assertEqual(p._body, '')

    def test_17_notifyNodeInformation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        node_list = [(CLIENT_NODE_TYPE, "127.0.0.1", 1, uuid1, RUNNING_STATE),
                       (CLIENT_NODE_TYPE, "127.0.0.2", 2, uuid2, DOWN_STATE),
                       (CLIENT_NODE_TYPE, "127.0.0.3", 3, uuid3, BROKEN_STATE)]
        p.notifyNodeInformation(1,
                                node_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), NOTIFY_NODE_INFORMATION)
        len_node_list  = unpack('!L', p._body[:4])[0]
        self.assertEqual(len_node_list, len(node_list))
        for x in xrange(len_node_list):
            node_type, ip_address, port, uuid, state = unpack('!H4sH16sH', p._body[4+x*26:30+x*26])
            ip_address = inet_ntoa(ip_address)
            node = (node_type, ip_address, port, uuid, state)
            self.failUnless(node in node_list)
            node_list.remove(node)
        self.assertEqual(len(node_list), 0)

    def test_18_askLastIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askLastIDs(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_LAST_IDS)
        self.assertEqual(p._body, '')

    def test_19_answerLastIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        tid = self.getNextTID()
        ptid = self.getNextTID()
        p.answerLastIDs(1, oid, tid, ptid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_LAST_IDS)
        loid, ltid, lptid = unpack('!8s8s8s', p._body)
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_20_askPartitionTable(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        offset_list = [1, 523, 6, 124]
        p.askPartitionTable(1,
                           offset_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_PARTITION_TABLE)
        len_offset_list  = unpack('!L', p._body[:4])[0]
        self.assertEqual(len_offset_list, len(offset_list))
        for x in xrange(len_offset_list):
            offset = unpack('!L', p._body[4+x*4:8+x*4])[0]
            self.failUnless(offset in offset_list)
            offset_list.remove(offset)
        self.assertEqual(len(offset_list), 0)

    def test_21_answerPartitionTable(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [[0, [(uuid1, UP_TO_DATE_STATE), (uuid2, OUT_OF_DATE_STATE)]],
                     [43, [(uuid2, OUT_OF_DATE_STATE),(uuid3, DISCARDED_STATE)]],
                     [124, [(uuid1, DISCARDED_STATE), (uuid3, UP_TO_DATE_STATE)]]]
        p.answerPartitionTable(1, ptid,
                                 cell_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_PARTITION_TABLE)
        pptid, len_cell_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(pptid, ptid)
        self.assertEqual(len_cell_list, len(cell_list))
        idx = 12
        for i in xrange(len_cell_list):
            offset, len_cell = unpack('!LL', p._body[idx:idx+8])
            idx += 8
            self.assertEqual(len_cell, 2)
            p_cell_list = []
            for j in xrange(len_cell):
                p_cell = unpack('!16sH', p._body[idx:idx+18])
                idx += 18
                p_cell_list.append(p_cell)
            cell = [offset, p_cell_list]
            self.failUnless(cell in cell_list)
            cell_list.remove(cell)
        self.assertEqual(len(cell_list), 0)


    def test_22_sendPartitionTable(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [[0, [(uuid1, UP_TO_DATE_STATE), (uuid2, OUT_OF_DATE_STATE)]],
                     [43, [(uuid2, OUT_OF_DATE_STATE),(uuid3, DISCARDED_STATE)]],
                     [124, [(uuid1, DISCARDED_STATE), (uuid3, UP_TO_DATE_STATE)]]]
        p.answerPartitionTable(1, ptid,
                                 cell_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_PARTITION_TABLE)
        pptid, len_cell_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(pptid, ptid)
        self.assertEqual(len_cell_list, len(cell_list))
        idx = 12
        for i in xrange(len_cell_list):
            offset, len_cell = unpack('!LL', p._body[idx:idx+8])
            idx += 8
            self.assertEqual(len_cell, 2)
            p_cell_list = []
            for j in xrange(len_cell):
                p_cell = unpack('!16sH', p._body[idx:idx+18])
                idx += 18
                p_cell_list.append(p_cell)
            cell = [offset, p_cell_list]
            self.failUnless(cell in cell_list)
            cell_list.remove(cell)
        self.assertEqual(len(cell_list), 0)

    def test_23_notifyPartitionChanges(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, uuid1, UP_TO_DATE_STATE),
                     (43, uuid2, OUT_OF_DATE_STATE),
                     (124, uuid1, DISCARDED_STATE)]
        p.notifyPartitionChanges(1, ptid,
                                 cell_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), NOTIFY_PARTITION_CHANGES)
        pptid, len_cell_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(pptid, ptid)
        self.assertEqual(len_cell_list, len(cell_list))
        for i in xrange(len_cell_list):
            cell = unpack('!L16sH', p._body[12+i*22:34+i*22])
            self.failUnless(cell in cell_list)
            cell_list.remove(cell)
        self.assertEqual(len(cell_list), 0)

    def test_24_startOperation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.startOperation(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), START_OPERATION)
        self.assertEqual(p._body, '')

    def test_25_stopOperation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.stopOperation(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), STOP_OPERATION)
        self.assertEqual(p._body, '')

    def test_26_askUnfinishedTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askUnfinishedTransactions(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_UNFINISHED_TRANSACTIONS)
        self.assertEqual(p._body, '')

    def test_27_answerUnfinishedTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerUnfinishedTransactions(1,tid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_UNFINISHED_TRANSACTIONS)
        len_tid_list  = unpack('!L', p._body[:4])[0]
        self.assertEqual(len_tid_list, len(tid_list))
        for i in xrange(len_tid_list):
            tid = unpack('!8s', p._body[4+i*8:12+i*8])[0]
            self.failUnless(tid in tid_list)
            tid_list.remove(tid)
        self.assertEqual(len(tid_list), 0)

    def test_28_askObjectPresent(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.askObjectPresent(1, oid, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_OBJECT_PRESENT)
        loid, ltid = unpack('!8s8s', p._body)
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_29_answerObjectPresent(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.answerObjectPresent(1, oid, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_OBJECT_PRESENT)
        loid, ltid = unpack('!8s8s', p._body)
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_30_deleteTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.deleteTransaction(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), DELETE_TRANSACTION)
        self.assertEqual(p._body, tid)

    def test_31_commitTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.commitTransaction(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), COMMIT_TRANSACTION)
        self.assertEqual(p._body, tid)

    def test_32_askNewTID(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askNewTID(1)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_NEW_TID)
        self.assertEqual(p._body, '')

    def test_33_answerNewTID(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.answerNewTID(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_NEW_TID)
        self.assertEqual(p._body, tid)

    def test_34_askNewOIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askNewOIDs(1, 10)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_NEW_OIDS)
        nb = unpack("!H", p._body)
        self.assertEqual(nb, (10,))

    def test_35_answerNewOIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerNewOIDs(1,oid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_NEW_OIDS)
        len_oid_list  = unpack('!H', p._body[:2])[0]
        self.assertEqual(len_oid_list, len(oid_list))
        for i in xrange(len_oid_list):
            oid = unpack('!8s', p._body[2+i*8:10+i*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_36_finishTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.finishTransaction(1,oid_list, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), FINISH_TRANSACTION)
        ptid, len_oid_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(ptid, tid)
        self.assertEqual(len_oid_list, len(oid_list))
        for i in xrange(len_oid_list):
            oid = unpack('!8s', p._body[12+i*8:20+i*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_37_notifyTransactionFinished(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.notifyTransactionFinished(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), NOTIFY_TRANSACTION_FINISHED)
        self.assertEqual(p._body, tid)

    def test_38_lockInformation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.lockInformation(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), LOCK_INFORMATION)
        self.assertEqual(p._body, tid)

    def test_39_notifyInformationLocked(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.notifyInformationLocked(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), NOTIFY_INFORMATION_LOCKED)
        self.assertEqual(p._body, tid)

    def test_40_invalidateObjects(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.invalidateObjects(1,oid_list, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), INVALIDATE_OBJECTS)
        ptid, len_oid_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(ptid, tid)
        self.assertEqual(len_oid_list, len(oid_list))
        for i in xrange(len_oid_list):
            oid = unpack('!8s', p._body[12+i*8:20+i*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_41_unlockInformation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.unlockInformation(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), UNLOCK_INFORMATION)
        self.assertEqual(p._body, tid)

    def test_42_abortTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.abortTransaction(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ABORT_TRANSACTION)
        self.assertEqual(p._body, tid)

    def test_43_askStoreTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.askStoreTransaction(1,
                              tid,
                              "moi",
                              "transaction",
                              "exti",
                              oid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_STORE_TRANSACTION)
        ptid, len_oid_list, len_user, len_desc, len_ext = unpack('!8sLHHH', p._body[:18])
        self.assertEqual(ptid, tid)
        self.assertEqual(len_user, len("moi"))
        self.assertEqual(len_desc, len("transaction"))
        self.assertEqual(len_ext, len("exti"))
        self.assertEqual(len_oid_list, len(oid_list))
        offset = 18
        user = p._body[offset:offset+len_user]
        self.assertEqual(user, "moi")
        offset += len_user
        desc = p._body[offset:offset+len_desc]
        self.assertEqual(desc, "transaction")
        offset += len_desc
        ext = p._body[offset:offset+len_ext]
        self.assertEqual(ext, "exti")
        offset += len_ext
        for x in xrange(len_oid_list):
            oid = unpack('8s', p._body[offset+x*8:offset+8+x*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_44_answerStoreTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.answerStoreTransaction(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_STORE_TRANSACTION)
        self.assertEqual(p._body, tid)

    def test_45_askStoreObject(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p.askStoreObject(1,
                         oid,
                         serial,
                         1,
                         55,
                         "to",
                         tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_STORE_OBJECT)
        poid, pserial, ptid, compression, checksum, len_data= unpack('!8s8s8sBLL', p._body[:33])
        data = p._body[-len_data:]
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")
        self.assertEqual(len_data, len("to"))

    def test_46_answerStoreObject(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        serial = self.getNextTID()
        p.answerStoreObject(1,
                            1,
                            oid,
                            serial)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_STORE_OBJECT)
        conflicting, poid, pserial = unpack('!B8s8s', p._body)
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(conflicting, 1)

    def test_47_askObject(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p.askObject(1,
                    oid,
                    serial,
                    tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_OBJECT)
        poid, pserial, ptid = unpack('!8s8s8s', p._body)
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)

    def test_48_answerObject(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        p.answerObject(1,
                       oid,
                       serial_start,
                       serial_end,
                       1,
                       55,
                       "to",)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_OBJECT)
        poid, pserial_start, pserial_end, compression, checksum, len_data= unpack('!8s8s8sBLL', p._body[:33])
        data = p._body[-len_data:]
        self.assertEqual(oid, poid)
        self.assertEqual(serial_start, pserial_start)
        self.assertEqual(serial_end, pserial_end)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")
        self.assertEqual(len_data, len("to"))

    def test_49_askTIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askTIDs(1,
                 1,
                 10,
                 5)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_TIDS)
        first, last, partition = unpack('!QQL', p._body)
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_50_answerTIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerTIDs(1,tid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_TIDS)
        len_tid_list  = unpack('!L', p._body[:4])[0]
        self.assertEqual(len_tid_list, len(tid_list))
        for i in xrange(len_tid_list):
            tid = unpack('!8s', p._body[4+i*8:12+i*8])[0]
            self.failUnless(tid in tid_list)
            tid_list.remove(tid)
        self.assertEqual(len(tid_list), 0)

    def test_51_askTransactionInfomation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.askTransactionInformation(1, tid)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_TRANSACTION_INFORMATION)
        ptid = unpack('!8s', p._body)
        self.assertEqual((tid,), ptid)

    def test_52_answerTransactionInformation(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerTransactionInformation(1,
                                       tid,
                                       "moi",
                                       "transaction",
                                       "exti",
                                       oid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_TRANSACTION_INFORMATION)
        ptid, len_user, len_desc, len_ext, len_oid_list = unpack('!8sHHHL', p._body[:18])
        self.assertEqual(ptid, tid)
        self.assertEqual(len_user, len("moi"))
        self.assertEqual(len_desc, len("transaction"))
        self.assertEqual(len_ext, len("exti"))
        self.assertEqual(len_oid_list, len(oid_list))
        offset = 18
        user = p._body[offset:offset+len_user]
        self.assertEqual(user, "moi")
        offset += len_user
        desc = p._body[offset:offset+len_desc]
        self.assertEqual(desc, "transaction")
        offset += len_desc
        ext = p._body[offset:offset+len_ext]
        self.assertEqual(ext, "exti")
        offset += len_ext
        for x in xrange(len_oid_list):
            oid = unpack('8s', p._body[offset+x*8:offset+8+x*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_53_askObjectHistory(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        p.askObjectHistory(1,
                           oid,
                           1,
                           10,)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_OBJECT_HISTORY)
        poid, first, last = unpack('!8sQQ', p._body)
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(poid, oid)

    def test_54_answerObjectHistory(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid = self.getNextTID()
        hist1 = (self.getNextTID(), 15)
        hist2 = (self.getNextTID(), 353)
        hist3 = (self.getNextTID(), 326)
        hist4 = (self.getNextTID(), 652)
        hist_list = [hist1, hist2, hist3, hist4]
        p.answerObjectHistory(1, oid, hist_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_OBJECT_HISTORY)
        poid, len_hist_list  = unpack('!8sL', p._body[:12])
        self.assertEqual(len_hist_list, len(hist_list))
        self.assertEqual(oid, poid)
        for i in xrange(len_hist_list):
            hist = unpack('!8sL', p._body[12+i*12:24+i*12])
            self.failUnless(hist in hist_list)
            hist_list.remove(hist)
        self.assertEqual(len(hist_list), 0)

    def test_55_askOIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        p.askOIDs(1,
                 1,
                 10,
                 5)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_OIDS)
        first, last, partition = unpack('!QQL', p._body)
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_56_answerOIDs(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerOIDs(1,oid_list)
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ANSWER_OIDS)
        len_oid_list  = unpack('!L', p._body[:4])[0]
        self.assertEqual(len_oid_list, len(oid_list))
        for i in xrange(len_oid_list):
            oid = unpack('!8s', p._body[4+i*8:12+i*8])[0]
            self.failUnless(oid in oid_list)
            oid_list.remove(oid)
        self.assertEqual(len(oid_list), 0)

    def test_decode_02_error(self):
        p = Packet()
        p.error(1, 10, "error message")
        code, msg = p._decodeError()
        self.assertEqual(code, 10)
        self.assertEqual(msg, "error message")

    def test_decode_03_protocolError(self):
        p = Packet()
        p.protocolError(1, "bad protocol")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, PROTOCOL_ERROR_CODE)
        self.assertEqual(error_msg, "protocol error: bad protocol")

    def test_decode_04_internalError(self):
        p = Packet()
        p.internalError(1, "bad internal")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, INTERNAL_ERROR_CODE)
        self.assertEqual(error_msg, "internal error: bad internal")

    def test_decode_05_notReady(self):
        p = Packet()
        p.notReady(1, "wait")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, NOT_READY_CODE)
        self.assertEqual(error_msg, "not ready: wait")

    def test_decode_06_brokenNodeDisallowedError(self):
        p = Packet()
        p.brokenNodeDisallowedError(1, "broken")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, BROKEN_NODE_DISALLOWED_CODE)
        self.assertEqual(error_msg, "broken node disallowed error: broken")

    def test_decode_07_oidNotFound(self):
        p = Packet()
        p.oidNotFound(1, "no oid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_msg, "oid not found: no oid")

    def test_decode_08_oidNotFound(self):
        p = Packet()
        p.tidNotFound(1, "no tid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, TID_NOT_FOUND_CODE)
        self.assertEqual(error_msg, "tid not found: no tid")

    def test_decode_09_ping(self):
        p = Packet()
        p.ping(1)
        self.assertEqual(None, p.decode())

    def test_decode_10_pong(self):
        p = Packet()
        p.pong(1)
        self.assertEqual(None, p.decode())

    def test_decode_11_requestNodeIdentification(self):
        p = Packet()
        uuid = self.getNewUUID()
        p.requestNodeIdentification(1,
                                    CLIENT_NODE_TYPE,
                                    uuid,
                                    "127.0.0.1",
                                    9080,
                                    "unittest"
                                    )
        node, p_uuid, ip, port, name  = p.decode()
        self.assertEqual(node, CLIENT_NODE_TYPE)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(name, "unittest")

    def test_decode_12_acceptNodeIdentification(self):
        p = Packet()
        uuid = self.getNewUUID()
        p.acceptNodeIdentification(1,
                                   CLIENT_NODE_TYPE,
                                   uuid,
                                   "127.0.0.1",
                                   9080,
                                   10,
                                   20
                                   )
        node, p_uuid, ip, port, nb_partitions, nb_replicas  = p.decode()
        self.assertEqual(node, CLIENT_NODE_TYPE)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(nb_partitions, 10)
        self.assertEqual(nb_replicas, 20)

    def test_decode_13_askPrimaryMaster(self):
        p = Packet()
        p.askPrimaryMaster(1)
        self.assertEqual(None, p.decode())

    def test_decode_14_answerPrimaryMaster(self):
        p = Packet()
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        master_list = [("127.0.0.1", 1, uuid1),
                       ("127.0.0.2", 2, uuid2),
                       ("127.0.0.3", 3, uuid3)]
        p.answerPrimaryMaster(1,
                              uuid,
                              master_list
                              )
        primary_uuid, p_master_list  = p.decode()
        self.assertEqual(primary_uuid, uuid)
        self.assertEqual(master_list, p_master_list)

    def test_decode_15_announcePrimaryMaster(self):
        p = Packet()
        p.announcePrimaryMaster(1)
        self.assertEqual(p.decode(), None)

    def test_decode_16_reelectPrimaryMaster(self):
        p = Packet()
        p.reelectPrimaryMaster(1)
        self.assertEqual(p.decode(), None)

    def test_decode_17_notifyNodeInformation(self):
        p = Packet()
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        node_list = [(CLIENT_NODE_TYPE, "127.0.0.1", 1, uuid1, RUNNING_STATE),
                       (CLIENT_NODE_TYPE, "127.0.0.2", 2, uuid2, DOWN_STATE),
                       (CLIENT_NODE_TYPE, "127.0.0.3", 3, uuid3, BROKEN_STATE)]
        p.notifyNodeInformation(1,
                                node_list)
        p_node_list  = p.decode()[0]
        self.assertEqual(node_list, p_node_list)

    def test_decode_18_askLastIDs(self):
        p = Packet()
        p.askLastIDs(1)
        self.assertEqual(p.decode(), None)

    def test_decode_19_answerLastIDs(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        ptid = self.getNextTID()
        p.answerLastIDs(1, oid, tid, ptid)
        loid, ltid, lptid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_decode_20_askPartitionTable(self):
        p = Packet()
        offset_list = [1, 523, 6, 124]
        p.askPartitionTable(1,
                           offset_list)
        p_offset_list  = p.decode()[0]
        self.assertEqual(offset_list, p_offset_list)

    def test_decode_21_answerPartitionTable(self):
        p = Packet()
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, ((uuid1, UP_TO_DATE_STATE), (uuid2, OUT_OF_DATE_STATE))),
                     (43, ((uuid2, OUT_OF_DATE_STATE),(uuid3, DISCARDED_STATE))),
                     (124, ((uuid1, DISCARDED_STATE), (uuid3, UP_TO_DATE_STATE)))]
        p.answerPartitionTable(1, ptid,
                               cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_decode_22_sendPartitionTable(self):
        p = Packet()
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, ((uuid1, UP_TO_DATE_STATE), (uuid2, OUT_OF_DATE_STATE))),
                     (43, ((uuid2, OUT_OF_DATE_STATE),(uuid3, DISCARDED_STATE))),
                     (124, ((uuid1, DISCARDED_STATE), (uuid3, UP_TO_DATE_STATE)))]
        p.answerPartitionTable(1, ptid,
                                 cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_decode_23_notifyPartitionChanges(self):
        p = Packet()
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, uuid1, UP_TO_DATE_STATE),
                     (43, uuid2, OUT_OF_DATE_STATE),
                     (124, uuid1, DISCARDED_STATE)]
        p.notifyPartitionChanges(1, ptid,
                                 cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_decode_24_startOperation(self):
        p = Packet()
        p.startOperation(1)
        self.assertEqual(p.decode(), None)

    def test_decode_25_stopOperation(self):
        p = Packet()
        p.stopOperation(1)
        self.assertEqual(p.decode(), None)

    def test_decode_26_askUnfinishedTransaction(self):
        p = Packet()
        p.askUnfinishedTransactions(1)
        self.assertEqual(p.decode(), None)

    def test_decode_27_answerUnfinishedTransaction(self):
        p = Packet()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerUnfinishedTransactions(1,tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_decode_28_askObjectPresent(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.askObjectPresent(1, oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_decode_29_answerObjectPresent(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.answerObjectPresent(1, oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_decode_30_deleteTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.deleteTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_31_commitTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.commitTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)


    def test_decode_32_askNewTID(self):
        p = Packet()
        p.askNewTID(1)
        self.assertEqual(p.decode(), None)

    def test_decode_33_answerNewTID(self):
        p = Packet()
        tid = self.getNextTID()
        p.answerNewTID(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_34_askNewOIDs(self):
        p = Packet()
        p.askNewOIDs(1, 10)
        nb = p.decode()
        self.assertEqual(nb, (10,))

    def test_decode_35_answerNewOIDs(self):
        p = Packet()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerNewOIDs(1,oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

    def test_decode_36_finishTransaction(self):
        p = Packet()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.finishTransaction(1,oid_list, tid)
        p_oid_list, ptid  = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_decode_37_notifyTransactionFinished(self):
        p = Packet()
        tid = self.getNextTID()
        p.notifyTransactionFinished(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_38_lockInformation(self):
        p = Packet()
        tid = self.getNextTID()
        p.lockInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_39_notifyInformationLocked(self):
        p = Packet()
        tid = self.getNextTID()
        p.notifyInformationLocked(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_40_invalidateObjects(self):
        p = Packet()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.invalidateObjects(1,oid_list, tid)
        p_oid_list, ptid  = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_decode_41_unlockInformation(self):
        p = Packet()
        tid = self.getNextTID()
        p.unlockInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_42_abortTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.abortTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_43_askStoreTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.askStoreTransaction(1,
                              tid,
                              "moi",
                              "transaction",
                              "exti",
                              oid_list)
        ptid, user, desc, ext, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")

    def test_decode_44_answerStoreTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.answerStoreTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_decode_45_askStoreObject(self):
        p = Packet()
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p.askStoreObject(1,
                         oid,
                         serial,
                         1,
                         55,
                         "to",
                         tid)
        poid, pserial, compression, checksum, data, ptid = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")

    def test_decode_46_answerStoreObject(self):
        p = Packet()
        oid = self.getNextTID()
        serial = self.getNextTID()
        p.answerStoreObject(1,
                            1,
                            oid,
                            serial)
        conflicting, poid, pserial = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(conflicting, 1)

    def test_decode_47_askObject(self):
        p = Packet()
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p.askObject(1,
                    oid,
                    serial,
                    tid)
        poid, pserial, ptid = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)

    def test_decode_48_answerObject(self):
        p = Packet()
        oid = self.getNextTID()
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        p.answerObject(1,
                       oid,
                       serial_start,
                       serial_end,
                       1,
                       55,
                       "to",)
        poid, pserial_start, pserial_end, compression, checksum, data= p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial_start, pserial_start)
        self.assertEqual(serial_end, pserial_end)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")

    def test_decode_49_askTIDs(self):
        p = Packet()
        p.askTIDs(1,
                 1,
                 10,
                 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_decode_50_answerTIDs(self):
        p = Packet()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerTIDs(1,tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_decode_51_askTransactionInfomation(self):
        p = Packet()
        tid = self.getNextTID()
        p.askTransactionInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(tid, ptid)

    def test_decode_52_answerTransactionInformation(self):
        p = Packet()
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerTransactionInformation(1,
                                       tid,
                                       "moi",
                                       "transaction",
                                       "exti",
                                       oid_list)
        ptid, user, desc, ext, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")

    def test_decode_53_askObjectHistory(self):
        p = Packet()
        oid = self.getNextTID()
        p.askObjectHistory(1,
                           oid,
                           1,
                           10,)
        poid, first, last = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(poid, oid)

    def test_decode_54_answerObjectHistory(self):
        p = Packet()
        oid = self.getNextTID()
        hist1 = (self.getNextTID(), 15)
        hist2 = (self.getNextTID(), 353)
        hist3 = (self.getNextTID(), 326)
        hist4 = (self.getNextTID(), 652)
        hist_list = [hist1, hist2, hist3, hist4]
        p.answerObjectHistory(1, oid, hist_list)
        poid, p_hist_list  = p.decode()
        self.assertEqual(p_hist_list, hist_list)
        self.assertEqual(oid, poid)

    def test_decode_55_askOIDs(self):
        p = Packet()
        p.askOIDs(1,
                 1,
                 10,
                 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_decode_56_answerOIDs(self):
        p = Packet()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerOIDs(1,oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

if __name__ == '__main__':
    unittest.main()

