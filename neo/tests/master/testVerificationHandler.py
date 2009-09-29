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
from neo import logging
from mock import Mock
from struct import pack, unpack
import neo
from neo.tests import NeoTestBase
from neo.protocol import Packet, INVALID_UUID
from neo.master.handlers.verification import VerificationHandler
from neo.master.app import Application
from neo import protocol
from neo.protocol import ERROR, ANNOUNCE_PRIMARY_MASTER, \
    NOTIFY_NODE_INFORMATION, ANSWER_LAST_IDS, ANSWER_PARTITION_TABLE, \
     ANSWER_UNFINISHED_TRANSACTIONS, ANSWER_OBJECT_PRESENT, \
     ANSWER_TRANSACTION_INFORMATION, OID_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import OperationFailure, ElectionFailure, VerificationFailure     
from neo.tests import DoNothingConnector
from neo.connection import ClientConnection


class MasterVerificationTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(**config)
        self.app.pt.clear()
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.createMaster(server=server)
        self.verification = VerificationHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        self.app.asking_uuid_dict = {}
        self.app.unfinished_tid_set = set()
        self.app.loid = '\0' * 8
        self.app.ltid = '\0' * 8
        for node in self.app.nm.getMasterNodeList():
            self.app.unconnected_master_node_set.add(node.getServer())
            node.setState(RUNNING_STATE)

        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10011
        self.master_address = ('127.0.0.1', self.master_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    def identifyToMasterNode(self, node_type=STORAGE_NODE_TYPE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        uuid = self.getNewUUID()
        self.app.nm.createFromNodeType(
            node_type, 
            server=(ip, port),
            uuid=uuid,
        )
        return uuid

    # Tests
    def test_01_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.connectionClosed(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_02_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.timeoutExpired(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_03_peerBroken(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.peerBroken(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), BROKEN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_09_handleAnswerLastIDs(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
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
        self.failUnless(new_ptid > self.app.pt.getID())
        self.failUnless(new_oid > self.app.loid)
        self.failUnless(new_tid > self.app.ltid)
        self.assertRaises(VerificationFailure, verification.handleAnswerLastIDs, conn, packet, new_oid, new_tid, new_ptid)
        self.assertNotEquals(new_oid, self.app.loid)
        self.assertNotEquals(new_tid, self.app.ltid)
        self.assertNotEquals(new_ptid, self.app.pt.getID())

    def test_11_handleAnswerUnfinishedTransactions(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_UNFINISHED_TRANSACTIONS)
        # do nothing
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_tid_set), 0)
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        verification.handleAnswerUnfinishedTransactions(conn, packet, [new_tid])
        self.assertEquals(len(self.app.unfinished_tid_set), 0)        
        # update dict
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_tid_set), 0)
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        verification.handleAnswerUnfinishedTransactions(conn, packet, [new_tid,])
        self.assertTrue(self.app.asking_uuid_dict[uuid])
        self.assertEquals(len(self.app.unfinished_tid_set), 1)
        self.assertTrue(new_tid in self.app.unfinished_tid_set)


    def test_12_handleAnswerTransactionInformation(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_TRANSACTION_INFORMATION)
        # do nothing, as unfinished_oid_set is None
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = False
        self.app.unfinished_oid_set  = None
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        oid = unpack('!Q', self.app.loid)[0]
        new_oid = pack('!Q', oid + 1)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(self.app.unfinished_oid_set, None)        
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = set()
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 1)
        self.assertTrue(new_oid in self.app.unfinished_oid_set)
        # do not work as oid is diff
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 1)
        old_oid = new_oid
        oid = unpack('!Q', old_oid)[0]
        new_oid = pack('!Q', oid + 1)
        self.assertNotEqual(new_oid, old_oid)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(self.app.unfinished_oid_set, None)

    def test_13_handleTidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=TID_NOT_FOUND_CODE)
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = []
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleTidNotFound(conn, packet, "msg")
        self.assertNotEqual(self.app.unfinished_oid_set, None)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.app.unfinished_oid_set  = []
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleTidNotFound(conn, packet, "msg")
        self.assertEqual(self.app.unfinished_oid_set, None)
        
    def test_14_handleAnswerObjectPresent(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_OBJECT_PRESENT)
        # do nothing as asking_uuid_dict is True
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        oid = unpack('!Q', self.app.loid)[0]
        new_oid = pack('!Q', oid + 1)
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleAnswerObjectPresent(conn, packet, new_oid, new_tid)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertFalse(self.app.asking_uuid_dict[uuid])
        verification.handleAnswerObjectPresent(conn, packet, new_oid, new_tid)
        self.assertTrue(self.app.asking_uuid_dict[uuid])
        
    def test_15_handleOidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=OID_NOT_FOUND_CODE)
        # do nothinf as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.object_present = True
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertTrue(self.app.object_present)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertFalse(self.app.asking_uuid_dict[uuid ])
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertFalse(self.app.object_present)
        self.assertTrue(self.app.asking_uuid_dict[uuid ])
    
if __name__ == '__main__':
    unittest.main()

