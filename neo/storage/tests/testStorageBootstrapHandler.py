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
import logging
import MySQLdb
from tempfile import mkstemp
from mock import Mock
from neo.master.app import MasterNode
from neo.pt import PartitionTable
from neo.storage.app import Application, StorageNode
from neo.storage.bootstrap import BootstrapEventHandler
from neo.storage.verification import VerificationEventHandler
from neo.protocol import STORAGE_NODE_TYPE, MASTER_NODE_TYPE
from neo.protocol import BROKEN_STATE, RUNNING_STATE, Packet, INVALID_UUID
from neo.protocol import ACCEPT_NODE_IDENTIFICATION, REQUEST_NODE_IDENTIFICATION
from neo.protocol import ERROR, BROKEN_NODE_DISALLOWED_CODE, ASK_PRIMARY_MASTER
from neo.protocol import ANSWER_PRIMARY_MASTER

SQL_ADMIN_USER = 'root'
SQL_ADMIN_PASSWORD = None
NEO_SQL_USER = 'test'
NEO_SQL_DATABASE = 'test_neo1'

class StorageBootstrapTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level = logging.ERROR)
        # create an application object
        config_file_text = """# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: 127.0.0.1:10010 
# The number of replicas.
replicas: 2
# The number of partitions.
partitions: 1009
# The name of this cluster.
name: main
# The user name for the database.
user: %(user)s
connector : SocketConnector
# The first master.
[mastertest]
server: 127.0.0.1:10010

[storagetest]
database: %(database)s
server: 127.0.0.1:10020
""" % {
    'database': NEO_SQL_DATABASE,
    'user': NEO_SQL_USER,
}
        # SQL connection
        connect_arg_dict = {'user': SQL_ADMIN_USER}
        if SQL_ADMIN_PASSWORD is not None:
            connect_arg_dict['passwd'] = SQL_ADMIN_PASSWORD
        sql_connection = MySQLdb.Connect(**connect_arg_dict)
        cursor = sql_connection.cursor()
        # new database
        cursor.execute('DROP DATABASE IF EXISTS %s' % (NEO_SQL_DATABASE, ))
        cursor.execute('CREATE DATABASE %s' % (NEO_SQL_DATABASE, ))
        cursor.execute('GRANT ALL ON %s.* TO "%s"@"localhost" IDENTIFIED BY ""' % 
                (NEO_SQL_DATABASE, NEO_SQL_USER))
        cursor.close()
        # config file
        tmp_id, self.tmp_path = mkstemp()
        tmp_file = os.fdopen(tmp_id, "w+b")
        tmp_file.write(config_file_text)
        tmp_file.close()
        self.app = Application(self.tmp_path, "storagetest")        
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.trying_master_node = self.app.nm.getMasterNodeList()[0 ]
        self.bootstrap = BootstrapEventHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.num_partitions = 1009
        self.num_replicas = 2
        
    def tearDown(self):
        # Delete tmp file
        os.remove(self.tmp_path)

    # Common methods
    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def getLastUUID(self):
        return self.uuid

    # Method to test the kind of packet returned in answer
    def checkCalledRequestNodeIdentification(self, conn, packet_number=0):
        """ Check Request Node Identification has been send"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 1)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), REQUEST_NODE_IDENTIFICATION)

    def checkCalledAbort(self, conn, packet_number=0):
        """Check the abort method has been called and an error packet has been sent"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1) # XXX required here ????
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 0)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ERROR)

    def checkNoPacketSent(self, conn):
        # no packet should be sent
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)

    # Tests
    def test_01_connectionCompleted(self):
        # trying mn is None -> RuntimeError
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.app.trying_master_node = None
        self.assertRaises(RuntimeError, self.bootstrap.connectionCompleted, conn)
        # request identification
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.connectionCompleted(conn)
        self.checkCalledRequestNodeIdentification(conn)

    def test_02_connectionFailed(self):
        # trying mn is None -> RuntimeError
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.app.trying_master_node = None
        self.assertRaises(RuntimeError, self.bootstrap.connectionFailed, conn)
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
        conn = Mock({
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        # the primary is not ready 
        self.app.trying_master_node = self.trying_master_node
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.handleNotReady(conn, None, None)
        self.assertEquals(self.app.primary_master_node, None)
        self.assertEquals(self.app.trying_master_node, None)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 1)
        # another master is not ready
        self.app.trying_master_node = self.trying_master_node
        master_node = MasterNode()
        self.app.primary_master_node = master_node
        self.bootstrap.handleNotReady(conn, None, None)
        self.assertEquals(self.app.primary_master_node, master_node)
        self.assertEquals(self.app.trying_master_node, None)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 2)
        self.checkNoPacketSent(conn)

    def test_08_handleRequestNodeIdentification1(self):
        # client socket connection -> rejected
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            name='',)
        self.checkCalledAbort(conn)
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 0)

    def test_08_handleRequestNodeIdentification2(self):
        # not a master node -> rejected
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), })
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet, 
            port=self.master_port,
            node_type=STORAGE_NODE_TYPE,
            ip_address='127.0.0.1',
            name=self.app.name,)
        self.checkCalledAbort(conn)
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 0)

    def test_08_handleRequestNodeIdentification3(self):
        # bad app name -> rejected
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), })
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            name='INVALID_NAME',)
        self.checkCalledAbort(conn)
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 0)

    def test_08_handleRequestNodeIdentification4(self):
        # new master
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": True,
            "getAddress" : ("192.168.1.1", self.master_port), })
        # master not known
        mn = self.app.nm.getNodeByServer(('192.168.1.1', self.master_port))
        self.assertEquals(mn, None)
        count = len(self.app.nm.getNodeList())
        uuid = self.getNewUUID()
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=uuid,
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='192.168.1.1',
            name=self.app.name,)
        self.assertEquals(len(self.app.nm.getNodeList()), count + 1)
        # check packet
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        call = conn.mockGetNamedCalls("addPacket")[0]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ACCEPT_NODE_IDENTIFICATION)
        # check connection uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), uuid)

    def test_08_handleRequestNodeIdentification5(self):
        # broken node -> rejected
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), })
        master = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        uuid=self.getNewUUID()
        master.setState(BROKEN_STATE)
        master.setUUID(uuid)
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=uuid,
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            name=self.app.name,)
        self.checkCalledAbort(conn)
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 0)

    def test_08_handleRequestNodeIdentification6(self):
        # master node is already known
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({"isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), })
        # master known
        mn = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        self.assertNotEquals(mn, None)
        uuid = self.getNewUUID()
        self.bootstrap.handleRequestNodeIdentification(
            conn=conn,
            uuid=uuid,
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='127.0.0.1',
            name=self.app.name)
        master = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        self.assertEquals(master.getUUID(), uuid)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 0)
        # packet
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        call = conn.mockGetNamedCalls("addPacket")[0]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ACCEPT_NODE_IDENTIFICATION)
        # connection uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), uuid)

    def test_09_handleAcceptNodeIdentification1(self):
        # server socket connection -> rejected
        conn = Mock({"isServerConnection": True,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
        self.app.trying_master_node = self.trying_master_node
        self.bootstrap.handleAcceptNodeIdentification(
            conn=conn,
            packet=packet,
            node_type=MASTER_NODE_TYPE,
            uuid=self.getNewUUID(),
            ip_address='127.0.0.1',
            port=self.master_port,
            num_partitions=self.app.num_partitions,
            num_replicas=self.app.num_replicas,
            your_uuid=self.getNewUUID())
        self.checkCalledAbort(conn)

    def test_09_handleAcceptNodeIdentification2(self):
        # not a master node -> rejected
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.storage_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
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
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 1)

    def test_09_handleAcceptNodeIdentification3(self):
        # bad address -> rejected
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
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
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 1)

    def test_09_handleAcceptNodeIdentification4(self):
        # bad number of replicas/partitions 
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
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
        self.assertRaises(
            RuntimeError, 
            self.bootstrap.handleAcceptNodeIdentification,
            num_partitions=self.app.num_partitions + 1,
            num_replicas=self.num_replicas,
            **args)
        self.assertRaises(
            RuntimeError, 
            self.bootstrap.handleAcceptNodeIdentification,
            num_partitions=self.app.num_partitions,
            num_replicas=self.num_replicas + 1,
            **args)
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)

    def test_09_handleAcceptNodeIdentification5(self):
        # no PT
        uuid, your_uuid = self.getNewUUID(), self.getNewUUID()
        self.app.num_partitions = None
        self.app.num_replicas = None
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        self.assertNotEquals(self.app.trying_master_node.getUUID(), uuid)
        self.assertNotEquals(self.app.trying_master_node.getUUID(), uuid)
        self.assertEqual(None, self.app.dm.getNumPartitions())
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
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
        self.assertEqual(self.num_partitions, self.app.dm.getNumPartitions())
        self.assertTrue(isinstance(self.app.pt, PartitionTable))
        self.assertEquals(self.app.ptid, self.app.dm.getPTID())
        # uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), uuid)
        self.assertEquals(self.app.trying_master_node.getUUID(), uuid)
        # packet
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        call = conn.mockGetNamedCalls("addPacket")[0]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ASK_PRIMARY_MASTER)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 1)
        
    def test_09_handleAcceptNodeIdentification6(self):
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        self.app.trying_master_node = self.trying_master_node
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
        uuid, your_uuid = self.getNewUUID(), self.getNewUUID()
        self.assertNotEquals(self.app.trying_master_node.getUUID(), uuid)
        self.assertEqual(None, self.app.dm.getNumPartitions())
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
        # uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), uuid)
        self.assertEquals(self.app.trying_master_node.getUUID(), uuid)
        # packet
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        call = conn.mockGetNamedCalls("addPacket")[0]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ASK_PRIMARY_MASTER)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 1)
        self.assertEqual(self.num_partitions, self.app.dm.getNumPartitions())
        
    def test_10_handleAnswerPrimaryMaster01(self):
        # server connection rejected
        conn = Mock({"isServerConnection": True,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
        self.app.trying_master_node = self.trying_master_node
        self.app.primary_master_node = None
        self.bootstrap.handleAnswerPrimaryMaster(
            conn=conn,
            packet=packet,
            primary_uuid=self.getNewUUID(),
            known_master_list=()
        )
        self.checkCalledAbort(conn)
        self.assertEquals(self.app.trying_master_node, self.trying_master_node)
        self.assertEquals(self.app.primary_master_node, None)

    def test_10_handleAnswerPrimaryMaster02(self):
        # register new master nodes
        existing_master = ('127.0.0.1', self.master_port, self.getNewUUID(), )
        new_master = ('192.168.0.1', 10001, self.getNewUUID(), )
        known_masters = (existing_master, new_master, )
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
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
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)
        
    def test_10_handleAnswerPrimaryMaster03(self):
        # invalid primary master uuid -> close connection
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
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
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)

    def test_10_handleAnswerPrimaryMaster04(self):
        # trying_master_node is not pmn -> close connection
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
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
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 1)
        self.assertEquals(self.app.trying_master_node, None)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)

    def test_10_handleAnswerPrimaryMaster05(self):
        # trying_master_node is pmn -> set verification handler
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
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
        self.assertEquals(len(conn.mockGetNamedCalls('setHandler')), 1)
        call = conn.mockGetNamedCalls('setHandler')[0]
        self.assertTrue(isinstance(call.getParam(0), VerificationEventHandler))
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)
        self.assertEquals(self.app.trying_master_node, pmn)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)

    def test_10_handleAnswerPrimaryMaster06(self):
        # primary_uuid not known -> nothing happen
        conn = Mock({"isServerConnection": False,
                    "getAddress" : ("127.0.0.1", self.master_port), })
        packet = Packet(msg_id=1, msg_type=ANSWER_PRIMARY_MASTER)
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
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)
    
if __name__ == "__main__":
    unittest.main()

