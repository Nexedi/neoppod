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
from neo.protocol import NodeTypes, NodeStates, CellStates, ClusterStates
from neo.protocol import ErrorCodes, Packets, Errors, LockState
from neo.protocol import INVALID_TID
from neo.tests import NeoTestBase

class ProtocolTests(NeoTestBase):

    def setUp(self):
        self.ltid = None

    def tearDown(self):
        pass

    def getNextTID(self):
        self.ltid = super(ProtocolTests, self).getNextTID(self.ltid)
        return self.ltid

    def test_03_protocolError(self):
        p = Errors.ProtocolError("bad protocol")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.PROTOCOL_ERROR)
        self.assertEqual(error_msg, "bad protocol")

    def test_05_notReady(self):
        p = Errors.NotReady("wait")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.NOT_READY)
        self.assertEqual(error_msg, "wait")

    def test_06_brokenNodeDisallowedError(self):
        p = Errors.Broken("broken")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.BROKEN_NODE)
        self.assertEqual(error_msg, "broken")

    def test_07_oidNotFound(self):
        p = Errors.OidNotFound("no oid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.OID_NOT_FOUND)
        self.assertEqual(error_msg, "no oid")

    def test_08_tidNotFound(self):
        p = Errors.TidNotFound("no tid")
        error_code, error_msg = p.decode()
        self.assertEqual(error_code, ErrorCodes.TID_NOT_FOUND)
        self.assertEqual(error_msg, "no tid")

    def test_09_ping(self):
        p = Packets.Ping()
        self.assertEqual(p.decode(), ())

    def test_10_pong(self):
        p = Packets.Pong()
        self.assertEqual(p.decode(), ())

    def test_11_RequestIdentification(self):
        uuid = self.getNewUUID()
        p = Packets.RequestIdentification(NodeTypes.CLIENT, uuid,
                                    ("127.0.0.1", 9080), "unittest")
        node, p_uuid, (ip, port), name  = p.decode()
        self.assertEqual(node, NodeTypes.CLIENT)
        self.assertEqual(p_uuid, uuid)
        self.assertEqual(ip, "127.0.0.1")
        self.assertEqual(port, 9080)
        self.assertEqual(name, "unittest")

    def test_12_AcceptIdentification(self):
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        p = Packets.AcceptIdentification(NodeTypes.CLIENT, uuid1,
            10, 20, uuid2)
        node, p_uuid, nb_partitions, nb_replicas, your_uuid  = p.decode()
        self.assertEqual(node, NodeTypes.CLIENT)
        self.assertEqual(p_uuid, uuid1)
        self.assertEqual(nb_partitions, 10)
        self.assertEqual(nb_replicas, 20)
        self.assertEqual(your_uuid, uuid2)

    def test_13_askPrimary(self):
        p = Packets.AskPrimary()
        self.assertEqual(p.decode(), ())

    def test_14_answerPrimary(self):
        uuid = self.getNewUUID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        master_list = [(("127.0.0.1", 1), uuid1),
                       (("127.0.0.2", 2), uuid2),
                       (("127.0.0.3", 3), uuid3)]
        p = Packets.AnswerPrimary(uuid, master_list)
        primary_uuid, p_master_list  = p.decode()
        self.assertEqual(primary_uuid, uuid)
        self.assertEqual(master_list, p_master_list)

    def test_15_announcePrimary(self):
        p = Packets.AnnouncePrimary()
        self.assertEqual(p.decode(), ())

    def test_16_reelectPrimary(self):
        p = Packets.ReelectPrimary()
        self.assertEqual(p.decode(), ())

    def test_17_notifyNodeInformation(self):
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        node_list = [(NodeTypes.CLIENT, ("127.0.0.1", 1), uuid1, NodeStates.RUNNING),
                       (NodeTypes.CLIENT, ("127.0.0.2", 2), uuid2, NodeStates.DOWN),
                       (NodeTypes.CLIENT, ("127.0.0.3", 3), uuid3, NodeStates.BROKEN)]
        p = Packets.NotifyNodeInformation(node_list)
        p_node_list = p.decode()[0]
        self.assertEqual(node_list, p_node_list)

    def test_18_askLastIDs(self):
        p = Packets.AskLastIDs()
        self.assertEqual(p.decode(), ())

    def test_19_answerLastIDs(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        ptid = self.getNextTID()
        p = Packets.AnswerLastIDs(oid, tid, ptid)
        loid, ltid, lptid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_20_askPartitionTable(self):
        self.assertEqual(Packets.AskPartitionTable().decode(), ())

    def test_21_answerPartitionTable(self):
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        uuid3 = self.getNewUUID()
        cell_list = [(0, ((uuid1, CellStates.UP_TO_DATE), (uuid2, CellStates.OUT_OF_DATE))),
                     (43, ((uuid2, CellStates.OUT_OF_DATE),(uuid3, CellStates.DISCARDED))),
                     (124, ((uuid1, CellStates.DISCARDED), (uuid3, CellStates.UP_TO_DATE)))]
        p = Packets.AnswerPartitionTable(ptid, cell_list)
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
        p = Packets.AnswerPartitionTable(ptid, cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_23_notifyPartitionChanges(self):
        ptid = self.getNextTID()
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        cell_list = [(0, uuid1, CellStates.UP_TO_DATE),
                     (43, uuid2, CellStates.OUT_OF_DATE),
                     (124, uuid1, CellStates.DISCARDED)]
        p = Packets.NotifyPartitionChanges(ptid,
                                 cell_list)
        pptid, p_cell_list  = p.decode()
        self.assertEqual(pptid, ptid)
        self.assertEqual(p_cell_list, cell_list)

    def test_24_startOperation(self):
        p = Packets.StartOperation()
        self.assertEqual(p.decode(), ())

    def test_25_stopOperation(self):
        p = Packets.StopOperation()
        self.assertEqual(p.decode(), ())

    def test_26_askUnfinishedTransaction(self):
        p = Packets.AskUnfinishedTransactions()
        self.assertEqual(p.decode(), ())

    def test_27_answerUnfinishedTransaction(self):
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid_list = [tid1, tid2, tid3, tid4]
        p = Packets.AnswerUnfinishedTransactions(tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_28_askObjectPresent(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        p = Packets.AskObjectPresent(oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_29_answerObjectPresent(self):
        oid = self.getNextTID()
        tid = self.getNextTID()
        p = Packets.AnswerObjectPresent(oid, tid)
        loid, ltid = p.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)

    def test_30_deleteTransaction(self):
        tid = self.getNextTID()
        p = Packets.DeleteTransaction(tid)
        self.assertEqual(p.getType(), Packets.DeleteTransaction)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_31_commitTransaction(self):
        tid = self.getNextTID()
        p = Packets.CommitTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)


    def test_32_askBeginTransaction(self):
        # try with an invalid TID, None must be returned
        tid = INVALID_TID
        p = Packets.AskBeginTransaction(tid)
        self.assertEqual(p.decode(), (None, ))
        # and with another TID
        tid = '\1' * 8
        p = Packets.AskBeginTransaction(tid)
        self.assertEqual(p.decode(), (tid, ))

    def test_33_answerBeginTransaction(self):
        tid = self.getNextTID()
        p = Packets.AnswerBeginTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_34_askNewOIDs(self):
        p = Packets.AskNewOIDs(10)
        nb = p.decode()
        self.assertEqual(nb, (10,))

    def test_35_answerNewOIDs(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = Packets.AnswerNewOIDs(oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

    def test_36_askFinishTransaction(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = Packets.AskFinishTransaction(tid, oid_list)
        p_tid, p_oid_list  = p.decode()
        self.assertEqual(p_tid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_37_answerTransactionFinished(self):
        tid = self.getNextTID()
        p = Packets.AnswerTransactionFinished(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_38_askLockInformation(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid_list = [oid1, oid2]
        tid = self.getNextTID()
        p = Packets.AskLockInformation(tid, oid_list)
        ptid, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(oid_list, p_oid_list)

    def test_39_answerInformationLocked(self):
        tid = self.getNextTID()
        p = Packets.AnswerInformationLocked(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_40_invalidateObjects(self):
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        tid = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = Packets.InvalidateObjects(tid, oid_list)
        p_tid, p_oid_list  = p.decode()
        self.assertEqual(p_tid, tid)
        self.assertEqual(p_oid_list, oid_list)

    def test_41_notifyUnlockInformation(self):
        tid = self.getNextTID()
        p = Packets.NotifyUnlockInformation(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_42_abortTransaction(self):
        tid = self.getNextTID()
        p = Packets.AbortTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_43_askStoreTransaction(self):
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = Packets.AskStoreTransaction(tid, "moi", "transaction", "exti", oid_list)
        ptid, user, desc, ext, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")

    def test_44_answerStoreTransaction(self):
        tid = self.getNextTID()
        p = Packets.AnswerStoreTransaction(tid)
        ptid = p.decode()[0]
        self.assertEqual(ptid, tid)

    def test_45_askStoreObject(self):
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p = Packets.AskStoreObject(oid, serial, 1, 55, "to", tid)
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
        p = Packets.AnswerStoreObject(1, oid, serial)
        conflicting, poid, pserial = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(conflicting, 1)

    def test_47_askObject(self):
        oid = self.getNextTID()
        serial = self.getNextTID()
        tid = self.getNextTID()
        p = Packets.AskObject(oid, serial, tid)
        poid, pserial, ptid = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial, pserial)
        self.assertEqual(tid, ptid)

    def test_48_answerObject(self):
        oid = self.getNextTID()
        serial_start = self.getNextTID()
        serial_end = self.getNextTID()
        data_serial = self.getNextTID()
        p = Packets.AnswerObject(oid, serial_start, serial_end, 1, 55, "to",
            data_serial)
        poid, pserial_start, pserial_end, compression, checksum, data, \
            pdata_serial = p.decode()
        self.assertEqual(oid, poid)
        self.assertEqual(serial_start, pserial_start)
        self.assertEqual(serial_end, pserial_end)
        self.assertEqual(compression, 1)
        self.assertEqual(checksum, 55)
        self.assertEqual(data, "to")
        self.assertEqual(pdata_serial, data_serial)

    def test_49_askTIDs(self):
        p = Packets.AskTIDs(1, 10, 5)
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
        p = Packets.AnswerTIDs(tid_list)
        p_tid_list  = p.decode()[0]
        self.assertEqual(p_tid_list, tid_list)

    def test_51_askTransactionInfomation(self):
        tid = self.getNextTID()
        p = Packets.AskTransactionInformation(tid)
        ptid = p.decode()[0]
        self.assertEqual(tid, ptid)

    def test_52_answerTransactionInformation(self):
        tid = self.getNextTID()
        oid1 = self.getNextTID()
        oid2 = self.getNextTID()
        oid3 = self.getNextTID()
        oid4 = self.getNextTID()
        oid_list = [oid1, oid2, oid3, oid4]
        p = Packets.AnswerTransactionInformation(tid, "moi",
                "transaction", "exti", False, oid_list)
        ptid, user, desc, ext, packed, p_oid_list = p.decode()
        self.assertEqual(ptid, tid)
        self.assertEqual(p_oid_list, oid_list)
        self.assertEqual(user, "moi")
        self.assertEqual(desc, "transaction")
        self.assertEqual(ext, "exti")
        self.assertFalse(packed)

    def test_53_askObjectHistory(self):
        oid = self.getNextTID()
        p = Packets.AskObjectHistory(oid, 1, 10,)
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
        p = Packets.AnswerObjectHistory(oid, hist_list)
        poid, p_hist_list  = p.decode()
        self.assertEqual(p_hist_list, hist_list)
        self.assertEqual(oid, poid)

    def test_55_askOIDs(self):
        p = Packets.AskOIDs(1, 10, 5)
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
        p = Packets.AnswerOIDs(oid_list)
        p_oid_list  = p.decode()[0]
        self.assertEqual(p_oid_list, oid_list)

    def test_57_notifyReplicationDone(self):
        offset = 10
        p = Packets.NotifyReplicationDone(offset)
        p_offset = p.decode()[0]
        self.assertEqual(p_offset, offset)

    def test_askUndoTransaction(self):
        tid = self.getNextTID()
        undone_tid = self.getNextTID()
        p = Packets.AskUndoTransaction(tid, undone_tid)
        p_tid, p_undone_tid = p.decode()
        self.assertEqual(p_tid, tid)
        self.assertEqual(p_undone_tid, undone_tid)

    def test_answerUndoTransaction(self):
        oid_list_1 = [self.getNextTID()]
        oid_list_2 = [self.getNextTID(), self.getNextTID()]
        oid_list_3 = [self.getNextTID(), self.getNextTID(), self.getNextTID()]
        p = Packets.AnswerUndoTransaction(oid_list_1, oid_list_2, oid_list_3)
        p_oid_list_1, p_oid_list_2, p_oid_list_3 = p.decode()
        self.assertEqual(p_oid_list_1, oid_list_1)
        self.assertEqual(p_oid_list_2, oid_list_2)
        self.assertEqual(p_oid_list_3, oid_list_3)

    def test_NotifyLastOID(self):
        oid = self.getOID(1)
        p = Packets.NotifyLastOID(oid)
        self.assertEqual(p.decode(), (oid, ))

    def test_AnswerClusterState(self):
        state = ClusterStates.RUNNING
        p = Packets.AnswerClusterState(state)
        self.assertEqual(p.decode(), (state, ))

    def test_AskClusterState(self):
        p = Packets.AskClusterState()
        self.assertEqual(p.decode(), ())

    def test_NotifyClusterInformation(self):
        state = ClusterStates.RECOVERING
        p = Packets.NotifyClusterInformation(state)
        self.assertEqual(p.decode(), (state, ))

    def test_SetClusterState(self):
        state = ClusterStates.VERIFYING
        p = Packets.SetClusterState(state)
        self.assertEqual(p.decode(), (state, ))

    def test_AnswerNodeInformation(self):
        p = Packets.AnswerNodeInformation()
        self.assertEqual(p.decode(), ())

    def test_AskNodeInformation(self):
        p = Packets.AskNodeInformation()
        self.assertEqual(p.decode(), ())

    def test_AnswerNewNodes(self):
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        p = Packets.AnswerNewNodes([uuid1, uuid2])
        self.assertEqual(p.decode(), ([uuid1, uuid2], ))

    def test_AddPendingNodes(self):
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        p = Packets.AddPendingNodes([uuid1, uuid2])
        self.assertEqual(p.decode(), ([uuid1, uuid2], ))

    def test_AnswerNodeState(self):
        uuid = self.getNewUUID()
        state = NodeStates.RUNNING
        p = Packets.AnswerNodeState(uuid, state)
        self.assertEqual(p.decode(), (uuid, state))

    def test_SetNodeState(self):
        uuid = self.getNewUUID()
        state = NodeStates.PENDING
        p = Packets.SetNodeState(uuid, state, True)
        self.assertEqual(p.decode(), (uuid, state, True))

    def test_AskNodeList(self):
        node_type = NodeTypes.STORAGE
        p = Packets.AskNodeList(node_type)
        self.assertEqual(p.decode(), (node_type, ))

    def test_AnswerNodeList(self):
        node1 = (NodeTypes.CLIENT, ('127.0.0.1', 1000),
                self.getNewUUID(), NodeStates.DOWN)
        node2 = (NodeTypes.MASTER, ('127.0.0.1', 2000),
                self.getNewUUID(), NodeStates.RUNNING)
        p = Packets.AnswerNodeList([node1, node2])
        self.assertEqual(p.decode(), ([node1, node2], ))

    def test_AskPartitionList(self):
        min_offset = 10
        max_offset = 20
        uuid = self.getNewUUID()
        p = Packets.AskPartitionList(min_offset, max_offset, uuid)
        self.assertEqual(p.decode(), (min_offset, max_offset, uuid))

    def test_AnswerPartitionList(self):
        ptid = self.getPTID(1)
        row_list = [
            (0, (
                (self.getNewUUID(), CellStates.UP_TO_DATE),
                (self.getNewUUID(), CellStates.OUT_OF_DATE),
                )),
            (1, (
                (self.getNewUUID(), CellStates.FEEDING),
                (self.getNewUUID(), CellStates.DISCARDED),
                )),
        ]
        p = Packets.AnswerPartitionList(ptid, row_list)
        self.assertEqual(p.decode(), (ptid, row_list))

    def test_AskHasLock(self):
        tid = self.getNextTID()
        oid = self.getNextTID()
        p = Packets.AskHasLock(tid, oid)
        self.assertEqual(p.decode(), (tid, oid))

    def test_AnswerHasLock(self):
        oid = self.getNextTID()
        for lock_state in LockState.itervalues():
            p = Packets.AnswerHasLock(oid, lock_state)
            self.assertEqual(p.decode(), (oid, lock_state))

if __name__ == '__main__':
    unittest.main()

