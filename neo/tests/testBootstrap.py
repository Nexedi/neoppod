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
from neo.tests import NeoTestBase
from neo.master.app import MasterNode
from neo.pt import PartitionTable
from neo.storage.app import Application, StorageNode
from neo.bootstrap import BootstrapManager
from neo import protocol
from neo.protocol import STORAGE_NODE_TYPE, MASTER_NODE_TYPE
from neo.protocol import BROKEN_STATE, RUNNING_STATE, Packet, INVALID_UUID
from neo.protocol import ACCEPT_NODE_IDENTIFICATION, REQUEST_NODE_IDENTIFICATION
from neo.protocol import ERROR, BROKEN_NODE_DISALLOWED_CODE, ASK_PRIMARY_MASTER
from neo.protocol import ANSWER_PRIMARY_MASTER

class BootstrapManagerTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.ERROR)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getConfigFile()
        self.app = Application(config, "master1")
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server=server))
        self.trying_master_node = self.app.nm.getMasterNodeList()[0]
        self.bootstrap = BootstrapManager(self.app, 'main', protocol.STORAGE_NODE_TYPE)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.num_partitions = 1009
        self.num_replicas = 2
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    # Tests
    def test_01_connectionCompleted(self):
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        # request identification
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.connectionCompleted(conn)
        self.checkAskPrimaryMaster(conn)

    def test_02_connectionFailed(self):
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        # the primary is dead
        self.app.trying_master_node = self.trying_master_node
        self.app.primary_master_node = self.app.trying_master_node
        self.bootstrap.connectionFailed(conn)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        # a master is dead
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.connectionFailed(conn)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)

    def test_03_connectionAccepted(self):
        # no packet sent
        uuid = self.getNewUUID()
        em = Mock({ 'register': None})
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port),
                     "getHandler" : self.bootstrap,
                     "getEventManager": em
        })
        connector = Mock({ })
        addr = ("127.0.0.1", self.master_port)
        self.bootstrap.connectionAccepted(conn, connector, addr)
        self.assertEquals(len(connector.mockGetNamedCalls('getEventManager')), 0)
        self.checkNoPacketSent(conn)

    def test_04_timeoutExpired(self):
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        # pmn connection has expired
        self.app.trying_master_node = self.trying_master_node
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.timeoutExpired(conn)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        # another master connection as expired
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.connectionFailed(conn)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkNoPacketSent(conn)

    def test_05_connectionClosed(self):
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        # pmn connection is closed
        self.app.trying_master_node = self.trying_master_node
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.connectionClosed(conn)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        # another master node connection is closed
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.connectionClosed(conn)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkNoPacketSent(conn)

    def test_06_peerBroken(self):
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        # the primary is broken 
        self.app.trying_master_node = self.trying_master_node
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.peerBroken(conn)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        # another master is broken
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.peerBroken(conn)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkNoPacketSent(conn)

    def test_07_handleNotReady(self):
        # the primary is not ready 
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.app.trying_master_node = self.trying_master_node
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.handleNotReady(conn, None, None)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkClosed(conn)
        # another master is not ready
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.handleNotReady(conn, None, None)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkClosed(conn)
        self.checkNoPacketSent(conn)

    def test_09_handleAcceptNodeIdentification2(self):
        # not a master node -> rejected
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.storage_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_type=ACCEPT_NODE_IDENTIFICATION)
        # non-master node to be removed
        server = ('127.0.0.1', self.storage_port)
        self.app.nm.add((StorageNode(server=server)))
        self.assertTrue(server in self.app.nm.server_dict)
        self.bootstrap.handleAcceptNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet,
            port=self.storage_port,
            node_type=STORAGE_NODE_TYPE,
            ip_address='127.0.0.1',
            num_partitions=self.num_partitions,
            num_replicas=self.app.num_replicas,
            your_uuid=self.getNewUUID())
        self.assertTrue(server not in self.app.nm.server_dict)
        self.checkClosed(conn)

    def test_09_handleAcceptNodeIdentification3(self):
        # bad address -> rejected
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_type=ACCEPT_NODE_IDENTIFICATION)
        self.bootstrap.handleAcceptNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet,
            port=self.storage_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            num_partitions=self.num_partitions,
            num_replicas=self.app.num_replicas,
            your_uuid=self.getNewUUID())
        server = ('127.0.0.1', self.master_port)
        self.assertTrue(server not in self.app.nm.server_dict)
        self.checkClosed(conn)

    def test_09_handleAcceptNodeIdentification4(self):
        # bad number of replicas/partitions 
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_type=ACCEPT_NODE_IDENTIFICATION)
        uuid = self.getNewUUID()
        args =  {
            'conn':conn,
            'uuid':uuid,
            'packet':packet,
            'port':self.master_port,
            'node_type':MASTER_NODE_TYPE,
            'ip_address':'127.0.0.1',
            'your_uuid': self.getNewUUID()
        }
        self.app.num_partitions = 1
        self.app.num_replicas = 1
        # partition number as changed -> error
        self.assertRaises(
            RuntimeError, 
            self.bootstrap.handleAcceptNodeIdentification,
            num_partitions=self.app.num_partitions + 2,
            num_replicas=self.app.num_replicas,
            **args)
        self.checkNoUUIDSet(conn)
        self.checkNoPacketSent(conn)

    def test_09_handleAcceptNodeIdentification5(self):
        # no errors
        uuid, your_uuid = self.getNewUUID(), self.getNewUUID()
        self.assertNotEquals(uuid, your_uuid)
        self.app.num_partitions = None # will create a partition table
        self.app.num_replicas = None
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        self.assertNotEquals(self.app.trying_master_node.getUUID(), uuid)
        self.assertEqual(self.app.dm.getNumPartitions(), self.num_partitions)
        packet = Packet(msg_type=ACCEPT_NODE_IDENTIFICATION)
        self.bootstrap.handleAcceptNodeIdentification(
            conn=conn,
            uuid=uuid,
            packet=packet,
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            num_partitions=self.num_partitions,
            num_replicas=self.num_replicas,
            your_uuid=your_uuid)
        # check PT
        self.assertEquals(self.app.num_partitions, self.num_partitions)
        self.assertEquals(self.app.num_replicas, self.num_replicas)
        self.assertEqual(self.app.num_partitions, self.app.dm.getNumPartitions())
        self.assertTrue(isinstance(self.app.pt, PartitionTable))
        self.assertEquals(self.app.ptid, self.app.dm.getPTID())
        self.checkAskPrimaryMaster(conn)
        # uuid
        self.checkUUIDSet(conn, uuid)
        self.assertEquals(self.app.trying_master_node.getUUID(), uuid)
        self.assertEquals(self.app.uuid, self.app.dm.getUUID())
        self.assertEquals(self.app.uuid, your_uuid)

    def test_10_handleAnswerPrimaryMaster02(self):
        # register new master nodes
        existing_master = ('127.0.0.1', self.master_port, self.getNewUUID(), )
        new_master = ('192.168.0.1', 10001, self.getNewUUID(), )
        known_masters = (existing_master, new_master, )
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_type=ANSWER_PRIMARY_MASTER)
        self.assertTrue(existing_master[:2] in self.app.nm.server_dict)
        self.assertTrue(new_master[:2] not in self.app.nm.server_dict)
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=self.getNewUUID(),
            known_master_list=known_masters
        )
        # check server list
        self.assertTrue(existing_master[:2] in self.app.nm.server_dict)
        self.assertTrue(new_master[:2] in self.app.nm.server_dict)
        # check new master
        n = self.app.nm.getNodeByServer(new_master[:2])
        self.assertTrue(isinstance(n, MasterNode))
        self.assertEquals(n.getUUID(), new_master[2])
        self.assertEquals(len(conn.mockGetNamedCalls('setHandler')), 0)
        self.checkRequestNodeIdentification(conn)
        
    def test_10_handleAnswerPrimaryMaster03(self):
        # invalid primary master uuid -> close connection
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_type=ANSWER_PRIMARY_MASTER)
        pmn = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        self.app.primary_master_node = pmn
        self.app.trying_master_node = pmn
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=INVALID_UUID,
            known_master_list=()
        )
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        self.assertEquals(len(conn.mockGetNamedCalls('setHandler')), 0)
        self.checkNoPacketSent(conn)
        self.checkClosed(conn)

    def test_10_handleAnswerPrimaryMaster04(self):
        # trying_master_node is not pmn -> close connection
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_type=ANSWER_PRIMARY_MASTER)
        pmn = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        pmn.setUUID(self.getNewUUID())
        self.app.primary_master_node = None
        self.app.trying_master_node = None
        self.assertNotEquals(pmn.getUUID(), None)
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=pmn.getUUID(),
            known_master_list=()
        )
        self.assertEquals(self.app.primary_master_node, pmn)
        self.assertEquals(len(conn.mockGetNamedCalls('setHandler')), 0)
        self.assertEquals(self.app.trying_master_node, None)
        self.checkNoPacketSent(conn)
        self.checkClosed(conn)

    def test_10_handleAnswerPrimaryMaster05(self):
        # trying_master_node is pmn -> set verification handler
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_type=ANSWER_PRIMARY_MASTER)
        pmn = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        pmn.setUUID(self.getNewUUID())
        self.app.primary_master_node = None
        self.app.trying_master_node = pmn
        self.assertNotEquals(pmn.getUUID(), None)
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=pmn.getUUID(),
            known_master_list=()
        )
        self.assertEquals(self.app.primary_master_node, pmn)
        self.assertEquals(self.app.trying_master_node, pmn)
        self.checkRequestNodeIdentification(conn)

    def test_10_handleAnswerPrimaryMaster06(self):
        # primary_uuid not known -> nothing happen
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_type=ANSWER_PRIMARY_MASTER)
        self.app.primary_master_node = None
        self.app.trying_master_node = None
        new_uuid = self.getNewUUID()
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=new_uuid,
            known_master_list=()
        )
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        self.assertEquals(len(conn.mockGetNamedCalls('setHandler')), 0)
        self.checkRequestNodeIdentification(conn)
    
if __name__ == "__main__":
    unittest.main()

