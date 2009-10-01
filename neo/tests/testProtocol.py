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
from neo import protocol
from neo.protocol import NodeTypes, NodeStates, CellStates
from neo.protocol import ErrorCodes, PacketTypes, Packet
from neo.protocol import INVALID_TID, PACKET_HEADER_SIZE
from neo.tests import NeoTestBase
from neo.util import getNextTID
from time import time, gmtime

class ProtocolTests(NeoTestBase):

    def setUp(self):
        self.ltid = INVALID_TID

    def tearDown(self):
        pass

    def getNextTID(self):
        self.ltid = getNextTID(self.ltid)
        return self.ltid

    def test_01_Packet_init(self):
        p = Packet(msg_type=PacketTypes.ASK_PRIMARY_MASTER, body=None)
        self.assertEqual(p.getType(), PacketTypes.ASK_PRIMARY_MASTER)
        self.assertEqual(len(p), PACKET_HEADER_SIZE)

    def test_02_error(self):
        p = protocol._error(0, "error message")
        code, msg = protocol._decodeError(p._body)
        self.assertEqual(code, ErrorCodes.NO_ERROR)
        self.assertEqual(msg, "error message")

    def test_03_protocolError(self):
        p = protocol.protocolError("bad protocol")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.PROTOCOL_ERROR)
        self.assertEqual(error_msg, "protocol error: bad protocol")

    def test_04_internalError(self):
        p = protocol.internalError("bad internal")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.INTERNAL_ERROR)
        self.assertEqual(error_msg, "internal error: bad internal")

    def test_05_notReady(self):
        p = protocol.notReady("wait")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.NOT_READY)
        self.assertEqual(error_msg, "not ready: wait")

    def test_06_brokenNodeDisallowedError(self):
        p = protocol.brokenNodeDisallowedError("broken")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.BROKEN_NODE)
        self.assertEqual(error_msg, "broken node disallowed error: broken")

    def test_07_oidNotFound(self):
        p = protocol.oidNotFound("no oid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_msg, "oid not found: no oid")

    def test_08_oidNotFound(self):
        p = protocol.tidNotFound("no tid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.TID_NOT_FOUND)
        self.assertEqual(error_msg, "tid not found: no tid")

    def test_09_ping(self):
        p = protocol.ping()
        self.assertEqual(None, p.decode())

    def test_10_pong(self):
        p = protocol.pong()
        self.assertEqual(None, p.decode())

    def test_11_requestNodeIdentification(self):
        uuid = self.getNewUUID()
        p = protocol.requestNodeIdentification(NodeTypes.CLIENT, uuid,
                                    ("127.0.0.1", 9080), "unittest")
        node, p_uuid, (ip, port), name  = p.decode()
        self.assertEqual(node, NodeTypes.CLIENT)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(name, "unittest")

    def test_12_acceptNodeIdentification(self):
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        p = protocol.acceptNodeIdentification(NodeTypes.CLIENT, uuid1,
                                   ("127.0.0.1", 9080), 10, 20, uuid2)
        node, p_uuid, (ip, port), nb_partitions, nb_replicas, your_uuid  = p.decode()
        self.assertEqual(node, NodeTypes.CLIENT)
        self.assertEqual(p_uuid, uuid1)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(nb_partitions, 10)
        self.assertEqual(nb_replicas, 20)
        self.assertEqual(your_uuid, uuid2)

    def test_13_askPrimaryMaster(self):
        p = protocol.askPrimaryMaster()
        self.assertEqual(None, p.decode())

    def test_14_answerPrimaryMaster(self):
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        master_list = [(("127.0.0.1", 1), uuid1),
                       (("127.0.0.2", 2), uuid2),
                       (("127.0.0.3", 3), uuid3)]
        p = protocol.answerPrimaryMaster(uuid, master_list)
        primary_uuid, p_master_list  = p.decode()
        self.assertEqual(primary_uuid, uuid)
        self.assertEqual(master_list, p_master_list)

    def test_15_announcePrimaryMaster(self):
        p = protocol.announcePrimaryMaster()
        self.assertEqual(p.decode(), None)

    def test_16_reelectPrimaryMaster(self):
        p = protocol.reelectPrimaryMaster()
        self.assertEqual(p.decode(), None)

    def test_17_notifyNodeInformation(self):
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        node_list = [(NodeTypes.CLIENT, ("127.0.0.1", 1), uuid1, NodeStates.RUNNING),
                       (NodeTypes.CLIENT, ("127.0.0.2", 2), uuid2, NodeStates.DOWN),
                       (NodeTypes.CLIENT, ("127.0.0.3", 3), uuid3, NodeStates.BROKEN)]
        p = protocol.notifyNodeInformation(node_list)
        p_node_list = p.decode()[0]
        self.assertEqual(node_list, p_node_list)

    def test_18_askLastIDs(self):
        p = protocol.askLastIDs()
        self.assertEqual(p.decode(), None)

    def test_19_answerLastIDs(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        ptid = self.getNextTID()
        p = protocol.answerLastIDs(oid, tid, ptid)
        loid, ltid, lptid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_20_askPartitionTable(self):
        offset_list = [1, 523, 6, 124]
        p = protocol.askPartitionTable(offset_list)
        p_offset_list  = p.decode()[0]
        self.assertEqual(offset_list, p_offset_list)

    def test_21_answerPartitionTable(self):
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, ((uuid1, CellStates.UP_TO_DATE), (uuid2, CellStates.OUT_OF_DATE))),
                     (43, ((uuid2, CellStates.OUT_OF_DATE),(uuid3, CellStates.DISCARDED))),
                     (124, ((uuid1, CellStates.DISCARDED), (uuid3, CellStates.UP_TO_DATE)))]
        p = protocol.answerPartitionTable(ptid, cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_22_sendPartitionTable(self):
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, ((uuid1, CellStates.UP_TO_DATE), (uuid2, CellStates.OUT_OF_DATE))),
                     (43, ((uuid2, CellStates.OUT_OF_DATE),(uuid3, CellStates.DISCARDED))),
                     (124, ((uuid1, CellStates.DISCARDED), (uuid3, CellStates.UP_TO_DATE)))]
        p = protocol.answerPartitionTable(ptid, cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_23_notifyPartitionChanges(self):
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, uuid1, CellStates.UP_TO_DATE),
                     (43, uuid2, CellStates.OUT_OF_DATE),
                     (124, uuid1, CellStates.DISCARDED)]
        p = protocol.notifyPartitionChanges(ptid,
                                 cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_24_startOperation(self):
        p = protocol.startOperation()
        self.assertEqual(p.decode(), None)

    def test_25_stopOperation(self):
        p = protocol.stopOperation()
        self.assertEqual(p.decode(), None)

    def test_26_askUnfinishedTransaction(self):
        p = protocol.askUnfinishedTransactions()
        self.assertEqual(p.decode(), None)

    def test_27_answerUnfinishedTransaction(self):
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p = protocol.answerUnfinishedTransactions(tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_28_askObjectPresent(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        p = protocol.askObjectPresent(oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_29_answerObjectPresent(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        p = protocol.answerObjectPresent(oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_30_deleteTransaction(self):
        tid = self.getNextTID()
        p = protocol.deleteTransaction(tid)
        self.assertEqual(p.getType(), PacketTypes.DELETE_TRANSACTION)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_31_commitTransaction(self):
        tid = self.getNextTID()
        p = protocol.commitTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)


    def test_32_askBeginTransaction(self):
        # try with an invalid TID, None must be returned
        tid = '\0' * 8
        p = protocol.askBeginTransaction(tid)
        self.assertEqual(p.decode(), (None, ))
        # and with another TID
        tid = '\1' * 8
        p = protocol.askBeginTransaction(tid)
        self.assertEqual(p.decode(), (tid, ))

    def test_33_answerBeginTransaction(self):
        tid = self.getNextTID()
        p = protocol.answerBeginTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_34_askNewOIDs(self):
        p = protocol.askNewOIDs(10)
        nb = p.decode()
        self.assertEqual(nb, (10,))

    def test_35_answerNewOIDs(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.answerNewOIDs(oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

    def test_36_finishTransaction(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.finishTransaction(oid_list, tid)
        p_oid_list, ptid  = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_37_notifyTransactionFinished(self):
        tid = self.getNextTID()
        p = protocol.notifyTransactionFinished(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_38_lockInformation(self):
        tid = self.getNextTID()
        p = protocol.lockInformation(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_39_notifyInformationLocked(self):
        tid = self.getNextTID()
        p = protocol.notifyInformationLocked(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_40_invalidateObjects(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.invalidateObjects(oid_list, tid)
        p_oid_list, ptid  = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_41_unlockInformation(self):
        tid = self.getNextTID()
        p = protocol.unlockInformation(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_42_abortTransaction(self):
        tid = self.getNextTID()
        p = protocol.abortTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_43_askStoreTransaction(self):
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.askStoreTransaction(tid, "moi", "transaction", "exti", oid_list)
        ptid, user, desc, ext, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")

    def test_44_answerStoreTransaction(self):
        tid = self.getNextTID()
        p = protocol.answerStoreTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_45_askStoreObject(self):
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p = protocol.askStoreObject(oid, serial, 1, 55, "to", tid)
        poid, pserial, compression, checksum, data, ptid = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")

    def test_46_answerStoreObject(self):
        oid = self.getNextTID()
        serial = self.getNextTID()
        p = protocol.answerStoreObject(1, oid, serial)
        conflicting, poid, pserial = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(conflicting, 1)

    def test_47_askObject(self):
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p = protocol.askObject(oid, serial, tid)
        poid, pserial, ptid = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)

    def test_48_answerObject(self):
        oid = self.getNextTID()
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        p = protocol.answerObject(oid, serial_start, serial_end, 1, 55, "to",)
        poid, pserial_start, pserial_end, compression, checksum, data= p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial_start, pserial_start)
        self.assertEqual(serial_end, pserial_end)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")

    def test_49_askTIDs(self):
        p = protocol.askTIDs(1, 10, 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_50_answerTIDs(self):
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p = protocol.answerTIDs(tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_51_askTransactionInfomation(self):
        tid = self.getNextTID()
        p = protocol.askTransactionInformation(tid)
        ptid = p.decode()[0]
        self.assertEqual(tid, ptid)

    def test_52_answerTransactionInformation(self):
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.answerTransactionInformation(tid, "moi", 
                "transaction", "exti", oid_list)
        ptid, user, desc, ext, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")

    def test_53_askObjectHistory(self):
        oid = self.getNextTID()
        p = protocol.askObjectHistory(oid, 1, 10,)
        poid, first, last = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(poid, oid)

    def test_54_answerObjectHistory(self):
        oid = self.getNextTID()
        hist1 = (self.getNextTID(), 15)
        hist2 = (self.getNextTID(), 353)
        hist3 = (self.getNextTID(), 326)
        hist4 = (self.getNextTID(), 652)
        hist_list = [hist1, hist2, hist3, hist4]
        p = protocol.answerObjectHistory(oid, hist_list)
        poid, p_hist_list  = p.decode()
        self.assertEqual(p_hist_list, hist_list)
        self.assertEqual(oid, poid)

    def test_55_askOIDs(self):
        p = protocol.askOIDs(1, 10, 5)
        first, last, partition = p.decode()
        self.assertEqual(first, 1)
        self.assertEqual(last, 10)
        self.assertEqual(partition, 5)

    def test_56_answerOIDs(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = protocol.answerOIDs(oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

if __name__ == '__main__':
    unittest.main()

