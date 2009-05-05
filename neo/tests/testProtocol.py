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
        self.assertEqual(p.getId(), 1)
        self.assertEqual(p.getType(), ASK_PRIMARY_MASTER)
        self.assertEqual(len(p), PACKET_HEADER_SIZE)

    def test_02_error(self):
        p = Packet()
        p.error(1, 10, "error message")
        code, msg = p._decodeError()
        self.assertEqual(code, 10)
        self.assertEqual(msg, "error message")

    def test_03_protocolError(self):
        p = Packet()
        p.protocolError(1, "bad protocol")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, PROTOCOL_ERROR_CODE)
        self.assertEqual(error_msg, "protocol error: bad protocol")

    def test_04_internalError(self):
        p = Packet()
        p.internalError(1, "bad internal")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, INTERNAL_ERROR_CODE)
        self.assertEqual(error_msg, "internal error: bad internal")

    def test_05_notReady(self):
        p = Packet()
        p.notReady(1, "wait")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, NOT_READY_CODE)
        self.assertEqual(error_msg, "not ready: wait")

    def test_06_brokenNodeDisallowedError(self):
        p = Packet()
        p.brokenNodeDisallowedError(1, "broken")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, BROKEN_NODE_DISALLOWED_CODE)
        self.assertEqual(error_msg, "broken node disallowed error: broken")

    def test_07_oidNotFound(self):
        p = Packet()
        p.oidNotFound(1, "no oid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_msg, "oid not found: no oid")

    def test_08_oidNotFound(self):
        p = Packet()
        p.tidNotFound(1, "no tid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, TID_NOT_FOUND_CODE)
        self.assertEqual(error_msg, "tid not found: no tid")

    def test_09_ping(self):
        p = Packet()
        p.ping(1)
        self.assertEqual(None, p.decode())

    def test_10_pong(self):
        p = Packet()
        p.pong(1)
        self.assertEqual(None, p.decode())

    def test_11_requestNodeIdentification(self):
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

    def test_12_acceptNodeIdentification(self):
        p = Packet()
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        p.acceptNodeIdentification(1,
                                   CLIENT_NODE_TYPE,
                                   uuid1,
                                   "127.0.0.1",
                                   9080,
                                   10,
                                   20,
                                   uuid2
                                   )
        node, p_uuid, ip, port, nb_partitions, nb_replicas, your_uuid  = p.decode()
        self.assertEqual(node, CLIENT_NODE_TYPE)
        self.assertEqual(p_uuid, uuid1)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(nb_partitions, 10)
        self.assertEqual(nb_replicas, 20)
        self.assertEqual(your_uuid, uuid2)

    def test_13_askPrimaryMaster(self):
        p = Packet()
        p.askPrimaryMaster(1)
        self.assertEqual(None, p.decode())

    def test_14_answerPrimaryMaster(self):
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

    def test_15_announcePrimaryMaster(self):
        p = Packet()
        p.announcePrimaryMaster(1)
        self.assertEqual(p.decode(), None)

    def test_16_reelectPrimaryMaster(self):
        p = Packet()
        p.reelectPrimaryMaster(1)
        self.assertEqual(p.decode(), None)

    def test_17_notifyNodeInformation(self):
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

    def test_18_askLastIDs(self):
        p = Packet()
        p.askLastIDs(1)
        self.assertEqual(p.decode(), None)

    def test_19_answerLastIDs(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        ptid = self.getNextTID()
        p.answerLastIDs(1, oid, tid, ptid)
        loid, ltid, lptid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_20_askPartitionTable(self):
        p = Packet()
        offset_list = [1, 523, 6, 124]
        p.askPartitionTable(1,
                           offset_list)
        p_offset_list  = p.decode()[0]
        self.assertEqual(offset_list, p_offset_list)

    def test_21_answerPartitionTable(self):
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

    def test_22_sendPartitionTable(self):
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

    def test_23_notifyPartitionChanges(self):
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

    def test_24_startOperation(self):
        p = Packet()
        p.startOperation(1)
        self.assertEqual(p.decode(), None)

    def test_25_stopOperation(self):
        p = Packet()
        p.stopOperation(1)
        self.assertEqual(p.decode(), None)

    def test_26_askUnfinishedTransaction(self):
        p = Packet()
        p.askUnfinishedTransactions(1)
        self.assertEqual(p.decode(), None)

    def test_27_answerUnfinishedTransaction(self):
        p = Packet()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerUnfinishedTransactions(1,tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_28_askObjectPresent(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.askObjectPresent(1, oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_29_answerObjectPresent(self):
        p = Packet()
        oid = self.getNextTID()
        tid = self.getNextTID()
        p.answerObjectPresent(1, oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_30_deleteTransaction(self):
        p = Packet()
        self.assertEqual(p.getId(), None)
        self.assertEqual(p.getType(), None)
        tid = self.getNextTID()
        p.deleteTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_31_commitTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.commitTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)


    def test_32_askNewTID(self):
        p = Packet()
        p.askNewTID(1)
        self.assertEqual(p.decode(), None)

    def test_33_answerNewTID(self):
        p = Packet()
        tid = self.getNextTID()
        p.answerNewTID(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_34_askNewOIDs(self):
        p = Packet()
        p.askNewOIDs(1, 10)
        nb = p.decode()
        self.assertEqual(nb, (10,))

    def test_35_answerNewOIDs(self):
        p = Packet()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p.answerNewOIDs(1,oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

    def test_36_finishTransaction(self):
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

    def test_37_notifyTransactionFinished(self):
        p = Packet()
        tid = self.getNextTID()
        p.notifyTransactionFinished(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_38_lockInformation(self):
        p = Packet()
        tid = self.getNextTID()
        p.lockInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_39_notifyInformationLocked(self):
        p = Packet()
        tid = self.getNextTID()
        p.notifyInformationLocked(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_40_invalidateObjects(self):
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

    def test_41_unlockInformation(self):
        p = Packet()
        tid = self.getNextTID()
        p.unlockInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_42_abortTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.abortTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_43_askStoreTransaction(self):
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

    def test_44_answerStoreTransaction(self):
        p = Packet()
        tid = self.getNextTID()
        p.answerStoreTransaction(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_45_askStoreObject(self):
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

    def test_46_answerStoreObject(self):
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

    def test_47_askObject(self):
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

    def test_48_answerObject(self):
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

    def test_49_askTIDs(self):
        p = Packet()
        p.askTIDs(1,
                 1,
                 10,
                 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_50_answerTIDs(self):
        p = Packet()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p.answerTIDs(1,tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_51_askTransactionInfomation(self):
        p = Packet()
        tid = self.getNextTID()
        p.askTransactionInformation(1, tid)
        ptid = p.decode()[0]
        self.assertEqual(tid, ptid)

    def test_52_answerTransactionInformation(self):
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

    def test_53_askObjectHistory(self):
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

    def test_54_answerObjectHistory(self):
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

    def test_55_askOIDs(self):
        p = Packet()
        p.askOIDs(1,
                 1,
                 10,
                 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_56_answerOIDs(self):
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

