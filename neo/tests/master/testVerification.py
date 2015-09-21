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
from struct import pack, unpack
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, NodeStates
from neo.master.verification import VerificationManager, VerificationFailure
from neo.master.app import Application


class MasterVerificationTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(config)
        self.app.pt.clear()
        self.verification = VerificationManager(self.app)
        self.app.loid = '\0' * 8
        self.app.tm.setLastTID('\0' * 8)
        for node in self.app.nm.getMasterList():
            self.app.unconnected_master_node_set.add(node.getAddress())
            node.setState(NodeStates.RUNNING)

        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10011
        self.master_address = ('127.0.0.1', self.master_port)
        self.storage_address = ('127.0.0.1', self.storage_port)

    def _tearDown(self, success):
        self.app.close()
        NeoUnitTestBase._tearDown(self, success)

    # Common methods
    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        uuid = self.getNewUUID(node_type)
        self.app.nm.createFromNodeType(
            node_type,
            address=(ip, port),
            uuid=uuid,
        )
        return uuid

    # Tests
    def test_01_connectionClosed(self):
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.UNKNOWN)
        self.assertRaises(VerificationFailure, self.verification.connectionClosed, conn)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.TEMPORARILY_DOWN)

    def _test_09_answerLastIDs(self):
        # XXX: test disabled, should be an unexpected packet
        verification = self.verification
        uuid = self.identifyToMasterNode()
        loid = self.app.loid
        ltid = self.app.tm.getLastTID()
        lptid = '\0' * 8
        # send information which are later to what PMN knows, this must raise
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        oid = unpack('!Q', loid)[0]
        new_oid = pack('!Q', oid + 1)
        upper, lower = unpack('!LL', ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.assertTrue(new_ptid > self.app.pt.getID())
        self.assertTrue(new_oid > self.app.loid)
        self.assertTrue(new_tid > self.app.tm.getLastTID())
        self.assertRaises(VerificationFailure, verification.answerLastIDs, conn, new_oid, new_tid, new_ptid)
        self.assertNotEqual(new_oid, self.app.loid)
        self.assertNotEqual(new_tid, self.app.tm.getLastTID())
        self.assertNotEqual(new_ptid, self.app.pt.getID())

    def test_11_answerUnfinishedTransactions(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        # do nothing
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.assertEqual(len(self.verification._tid_set), 0)
        new_tid = self.getNextTID()
        verification.answerUnfinishedTransactions(conn, new_tid, [new_tid])
        self.assertEqual(len(self.verification._tid_set), 0)
        # update dict
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.verification._uuid_set.add(uuid)
        self.assertEqual(len(self.verification._tid_set), 0)
        new_tid = self.getNextTID(new_tid)
        verification.answerUnfinishedTransactions(conn, new_tid, [new_tid])
        self.assertTrue(uuid not in self.verification._uuid_set)
        self.assertEqual(len(self.verification._tid_set), 1)
        self.assertTrue(new_tid in self.verification._tid_set)

    def test_12_answerTransactionInformation(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        # do nothing, as unfinished_oid_set is None
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        self.verification._oid_set  = None
        new_tid = self.getNextTID()
        new_oid = self.getOID(1)
        verification.answerTransactionInformation(conn, new_tid,
                "user", "desc", "ext", False, [new_oid,])
        self.assertEqual(self.verification._oid_set, None)
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._oid_set  = set()
        self.assertEqual(len(self.verification._oid_set), 0)
        verification.answerTransactionInformation(conn, new_tid,
                "user", "desc", "ext", False, [new_oid,])
        self.assertEqual(len(self.verification._oid_set), 0)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        self.assertEqual(len(self.verification._oid_set), 0)
        verification.answerTransactionInformation(conn, new_tid,
                "user", "desc", "ext", False, [new_oid,])
        self.assertEqual(len(self.verification._oid_set), 1)
        self.assertTrue(new_oid in self.verification._oid_set)
        # do not work as oid is diff
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        self.assertEqual(len(self.verification._oid_set), 1)
        new_oid = self.getOID(2)
        self.assertRaises(ValueError, verification.answerTransactionInformation,
                conn, new_tid, "user", "desc", "ext", False, [new_oid,])

    def test_13_tidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._oid_set  = []
        verification.tidNotFound(conn, "msg")
        self.assertNotEqual(self.verification._oid_set, None)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        self.verification._oid_set  = []
        verification.tidNotFound(conn, "msg")
        self.assertEqual(self.verification._oid_set, None)

    def test_14_answerObjectPresent(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        # do nothing as asking_uuid_dict is True
        new_tid = self.getNextTID()
        new_oid = self.getOID(1)
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        verification.answerObjectPresent(conn, new_oid, new_tid)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        verification.answerObjectPresent(conn, new_oid, new_tid)
        self.assertTrue(uuid not in self.verification._uuid_set)

    def test_15_oidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.app._object_present = True
        self.assertTrue(self.app._object_present)
        verification.oidNotFound(conn, "msg")
        self.assertTrue(self.app._object_present)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.verification._uuid_set), 0)
        self.verification._uuid_set.add(uuid)
        self.assertTrue(self.app._object_present)
        verification.oidNotFound(conn, "msg")
        self.assertFalse(self.app._object_present)
        self.assertTrue(uuid not in self.verification._uuid_set)

if __name__ == '__main__':
    unittest.main()

