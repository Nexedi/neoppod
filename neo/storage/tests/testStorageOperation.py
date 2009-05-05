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
from struct import pack, unpack
from mock import Mock
from collections import deque
from neo.master.app import MasterNode
from neo.storage.app import Application, StorageNode
from neo.storage.operation import TransactionInformation, OperationEventHandler
from neo.exception import PrimaryFailure, OperationFailure
from neo.pt import PartitionTable
from neo.protocol import *

SQL_ADMIN_USER = 'root'
SQL_ADMIN_PASSWORD = None
NEO_SQL_USER = 'test'
NEO_SQL_DATABASE = 'test_neo1'

class StorageOperationTests(unittest.TestCase):

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def getTwoIDs(self):
        # generate two ptid, first is lower
        ptids = self.getNewUUID(), self.getNewUUID()
        return min(ptids), max(ptids)
        ptid = min(ptids)

    def checkCalledAbort(self, conn, packet_number=0):
        """Check the abort method has been called and an error packet has been sent"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1) # XXX required here ????
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 0)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ERROR)

    def checkPacket(self, conn, packet_type=ERROR):
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        call = conn.mockGetNamedCalls("addPacket")[0]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), packet_type)

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = Mock({ 
            "getAddress" : ("127.0.0.1", self.master_port), 
            "isServerConnection": _listening,    
        })
        packet = Packet(msg_id=1, msg_type=_msg_type)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        _call(conn=conn, packet=packet, **kwargs)
        self.checkCalledAbort(conn)
        self.assertEquals(len(conn.mockGetNamedCalls("peerBrokendCalled")), 1)

    def checkNoPacketSent(self, conn):
        # no packet should be sent
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)

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
        # config file
        tmp_id, self.tmp_path = mkstemp()
        tmp_file = os.fdopen(tmp_id, "w+b")
        tmp_file.write(config_file_text)
        tmp_file.close()
        # main application
        self.app = Application(self.tmp_path, "storagetest")        
        self.app.num_partitions = 1
        self.app.num_replicas = 1
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        for server in self.app.master_node_list:
            master = MasterNode(server = server)
            self.app.nm.add(master)
        # handler
        self.operation = OperationEventHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterNodeList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def tearDown(self):
        os.remove(self.tmp_path)

    def test_01_TransactionInformation(self):
        uuid = self.getNewUUID()
        transaction = TransactionInformation(uuid)
        # uuid
        self.assertEquals(transaction._uuid, uuid)
        self.assertEquals(transaction.getUUID(), uuid)
        # objects
        self.assertEquals(transaction._object_dict, {})
        object = (self.getNewUUID(), 1, 2, 3, )
        transaction.addObject(*object)
        objects = transaction.getObjectList()
        self.assertEquals(len(objects), 1)
        self.assertEquals(objects[0], object)
        # transactions
        self.assertEquals(transaction._transaction, None)
        t = ((1, 2, 3), 'user', 'desc', '')
        transaction.addTransaction(*t)
        self.assertEquals(transaction.getTransaction(), t)

    def test_02_connectionCompleted(self):
        # not (yet) implemented
        conn = Mock()
        self.assertRaises(NotImplementedError, 
                self.operation.connectionCompleted, conn)

    def test_03_connectionFailed(self):
        # not (yet) implemented
        conn = Mock()
        self.assertRaises(NotImplementedError, 
                self.operation.connectionFailed, conn)

    def test_04_connectionAccepted(self):
        uuid = self.getNewUUID()
        event_manager = Mock({'register': None})
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port),
                     "getHandler" : self.operation,
                     "getEventManager": event_manager,
        })
        connector = Mock({ })
        addr = ("127.0.0.1", self.master_port)
        self.operation.connectionAccepted(conn, connector, addr)
        # check call to subclass
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("getEventManager")), 1)
        self.checkNoPacketSent(conn)

    def test_05_dealWithClientFailure(self):
        # check if client's transaction are cleaned
        uuid = self.getNewUUID()
        from neo.node import ClientNode
        client = ClientNode(('127.0.0.1', 10010))
        client.setUUID(uuid)
        self.app.nm.add(client)
        self.app.store_lock_dict[0] = object()
        transaction = Mock({
            'getUUID': uuid,
            'getObjectList': ((0, ), ),
        })
        self.app.transaction_dict[0] = transaction
        self.assertTrue(1 not in self.app.store_lock_dict)
        self.assertTrue(1 not in self.app.transaction_dict)
        self.operation.dealWithClientFailure(uuid)
        # objects and transaction removed
        self.assertTrue(0 not in self.app.store_lock_dict)
        self.assertTrue(0 not in self.app.transaction_dict)

    def test_06_timeoutExpired(self):
        # server connection
        conn = Mock({
            "isServerConnection": True, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.operation.timeoutExpired(conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)
        # client connection
        conn = Mock({
            "getUUID": self.master_uuid,
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(PrimaryFailure, self.operation.timeoutExpired, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)
        # connection with another storage node
        conn = Mock({
            "getUUID": self.getNewUUID(),
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(NotImplementedError, self.operation.timeoutExpired, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)

    def test_07_connectionClosed1(self):
        # server connection
        conn = Mock({
            "isServerConnection": True, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.operation.connectionClosed(conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)

    def test_07_connectionClosed2(self):
        # primary has closed the connection
        conn = Mock({
            "getUUID": self.master_uuid,
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(PrimaryFailure,
                self.operation.connectionClosed, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)

    def test_07_connectionClosed3(self):
        # listening connection with a storage node
        conn = Mock({
            "getUUID": self.getNewUUID(),
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(NotImplementedError, self.operation.connectionClosed, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)

    def test_08_peerBroken(self):
        # server connection
        conn = Mock({
            "isServerConnection": True, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.operation.peerBroken(conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)
        # client connection
        conn = Mock({
            "getUUID": self.master_uuid,
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(PrimaryFailure,
                self.operation.peerBroken, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)
        # connection with another storage node
        conn = Mock({
            "getUUID": self.getNewUUID(),
            "isServerConnection": False, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        self.assertRaises(NotImplementedError, self.operation.peerBroken, conn)
        self.assertEquals(len(conn.mockGetNamedCalls('isServerConnection')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('getUUID')), 1)
        self.checkNoPacketSent(conn)

    def test_09_handleRequestNodeIdentification1(self):
        # reject client connection
        count = len(self.app.nm.getNodeList())
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleRequestNodeIdentification, 
            _listening=False,
            _msg_type=REQUEST_NODE_IDENTIFICATION,
            node_type=MASTER_NODE_TYPE,
            uuid=self.getNewUUID(),
            ip_address='127.0.0.1',
            port=self.master_port,
            name=self.app.name)
        self.assertEquals(len(self.app.nm.getNodeList()), count)

    def test_09_handleRequestNodeIdentification2(self):
        # bad app name
        uuid = self.getNewUUID()
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({
            "getUUID": uuid,
            "isServerConnection": True, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        count = len(self.app.nm.getNodeList())
        self.operation.handleRequestNodeIdentification(
            conn=conn,
            packet=packet, 
            node_type=MASTER_NODE_TYPE, 
            uuid=uuid,
            ip_address='127.0.0.1',
            port=self.master_port, 
            name='INVALID_NAME')
        self.checkCalledAbort(conn)
        self.assertEquals(len(self.app.nm.getNodeList()), count)

    def test_09_handleRequestNodeIdentification3(self):
        # broken node
        uuid = self.getNewUUID()
        self.app.primary_master_node.setState(BROKEN_STATE)
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({
            "getUUID": uuid,
            "isServerConnection": True, 
            "getAddress" : ("127.0.0.1", self.master_port),
        })
        count = len(self.app.nm.getNodeList())
        self.operation.handleRequestNodeIdentification(
            conn=conn,
            packet=packet, 
            node_type=MASTER_NODE_TYPE, 
            uuid=self.master_uuid,
            ip_address='127.0.0.1',
            port=self.master_port, 
            name=self.app.name)
        self.checkPacket(conn, packet_type=ERROR)
        self.assertEquals(len(self.app.nm.getNodeList()), count)

    def test_09_handleRequestNodeIdentification4(self):
        # new non-master, rejected
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({
            "isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        count = len(self.app.nm.getNodeList())
        self.operation.handleRequestNodeIdentification(
            conn=conn,
            uuid=self.getNewUUID(),
            packet=packet, 
            port=self.master_port,
            node_type=STORAGE_NODE_TYPE,
            ip_address='192.168.1.1',
            name=self.app.name,)
        self.checkCalledAbort(conn)
        self.assertEquals(len(self.app.nm.getNodeList()), count)

    def test_09_handleRequestNodeIdentification5(self):
        # new master, accepted
        uuid = self.getNewUUID()
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({
            "isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        count = len(self.app.nm.getNodeList())
        self.operation.handleRequestNodeIdentification(
            conn=conn,
            uuid=uuid,
            packet=packet, 
            port=self.master_port,
            node_type=MASTER_NODE_TYPE,
            ip_address='192.168.1.1',
            name=self.app.name,)
        # node added
        self.assertEquals(len(self.app.nm.getNodeList()), count + 1)
        n = self.app.nm.getNodeByServer(('192.168.1.1', self.master_port))
        self.assertNotEquals(n, None)
        self.assertEquals(n.getUUID(), uuid)
        self.checkPacket(conn, packet_type=ACCEPT_NODE_IDENTIFICATION)
        # uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), uuid)
        # abort
        self.assertEquals(len(conn.mockGetNamedCalls('abort')), 1)

    def test_09_handleRequestNodeIdentification6(self):
        # not new & accepted
        packet = Packet(msg_id=1, msg_type=REQUEST_NODE_IDENTIFICATION)
        conn = Mock({
            "isServerConnection": True,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        mn = self.app.nm.getNodeByServer(('127.0.0.1', self.master_port))
        uuid = self.getNewUUID()
        mn.setUUID(uuid)
        count = len(self.app.nm.getNodeList())
        self.operation.handleRequestNodeIdentification(
            conn=conn,
            uuid=self.uuid,
            packet=packet, 
            port=self.master_port,
            node_type=STORAGE_NODE_TYPE,
            ip_address='127.0.0.1',
            name=self.app.name,)
        # no new node
        self.assertEquals(len(self.app.nm.getNodeList()), count)
        self.checkPacket(conn, packet_type=ACCEPT_NODE_IDENTIFICATION)
        # uuid
        self.assertEquals(len(conn.mockGetNamedCalls("setUUID")), 1)
        call = conn.mockGetNamedCalls("setUUID")[0]
        self.assertEquals(call.getParam(0), self.uuid)
        self.assertEquals(mn.getUUID(), self.uuid)
        # abort
        self.assertEquals(len(conn.mockGetNamedCalls('abort')), 0)

    def test_10_handleAcceptNodeIdentification1(self):
        # client connection not implemented
        conn = Mock({
            "isServerConnection": False,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        packet = Packet(msg_id=1, msg_type=ACCEPT_NODE_IDENTIFICATION)
        self.assertRaises(NotImplementedError,
            self.operation.handleAcceptNodeIdentification,
            conn=conn,
            packet=packet,
            node_type=MASTER_NODE_TYPE,
            uuid=self.getNewUUID(),
            ip_address='127.0.0.1',
            port=self.master_port,
            num_partitions=self.app.num_partitions,
            num_replicas=self.app.num_replicas,
            your_uuid=INVALID_UUID,
        )

    def test_10_handleAcceptNodeIdentification2(self):
        # server connection rejected
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAcceptNodeIdentification,
            _msg_type=ACCEPT_NODE_IDENTIFICATION,
            _listening=True, 
            node_type=MASTER_NODE_TYPE,
            uuid=self.getNewUUID(),
            ip_address='127.0.0.1',
            port=self.master_port,
            num_partitions=self.app.num_partitions,
            num_replicas=self.app.num_replicas,
            your_uuid=INVALID_UUID,
        )

    def test_11_handleAnswerPrimaryMaster(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAnswerPrimaryMaster,
            _msg_type=ANSWER_PRIMARY_MASTER,
            primary_uuid=self.getNewUUID(),
            known_master_list=()
        )

    def test_11_handleAskLastIDs(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskLastIDs,
            _msg_type=ASK_LAST_IDS,
        )

    def test_12_handleAskPartitionTable(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskPartitionTable,
            _msg_type=ASK_PARTITION_TABLE,
            offset_list=()
        )

    def test_13_handleSendPartitionTable(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleSendPartitionTable,
            _msg_type=SEND_PARTITION_TABLE,
            ptid=0,
            row_list=()
        )

    def test_14_handleNotifyPartitionChanges1(self):
        # reject server connection 
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleNotifyPartitionChanges,
            _msg_type=NOTIFY_PARTITION_CHANGES,
            ptid=0,
            cell_list=()
        )
        # old partition change -> do nothing
        app = self.app
        conn = Mock({
            "isServerConnection": False,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        app.replicator = Mock({})
        packet = Packet(msg_id=1, msg_type=NOTIFY_PARTITION_CHANGES)
        self.app.ptid = 1
        count = len(self.app.nm.getNodeList())
        self.operation.handleNotifyPartitionChanges(conn, packet, 0, ())
        self.assertEquals(self.app.ptid, 1)
        self.assertEquals(len(self.app.nm.getNodeList()), count)
        calls = self.app.replicator.mockGetNamedCalls('removePartition')
        self.assertEquals(len(calls), 0)
        calls = self.app.replicator.mockGetNamedCalls('addPartition')
        self.assertEquals(len(calls), 0)

    def test_14_handleNotifyPartitionChanges2(self):
        # cases :
        uuid = self.getNewUUID()
        cells = (
            (0, uuid, UP_TO_DATE_STATE),
            (1, self.app.uuid, DISCARDED_STATE),
            (2, self.app.uuid, OUT_OF_DATE_STATE),
        )
        # context
        conn = Mock({
            "isServerConnection": False,
            "getAddress" : ("127.0.0.1", self.master_port), 
        })
        packet = Packet(msg_id=1, msg_type=NOTIFY_PARTITION_CHANGES)
        app = self.app
        ptid1, ptid2 = self.getTwoIDs()
        self.assertNotEquals(ptid1, ptid2)
        app.ptid = ptid1
        app.pt = PartitionTable(3, 1)
        app.pt = Mock({ })
        app.dm = Mock({ })
        app.replicator = Mock({})
        count = len(app.nm.getNodeList())
        self.operation.handleNotifyPartitionChanges(conn, packet, ptid2, cells)
        # ptid set
        self.assertEquals(app.ptid, ptid2)
        # two nodes added 
        self.assertEquals(len(app.nm.getNodeList()), count + 2)
        # uuid != app.uuid -> TEMPORARILY_DOWN_STATE
        self.assertEquals(app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE)
        # pt calls
        calls = self.app.pt.mockGetNamedCalls('setCell')
        self.assertEquals(len(calls), 3)
        self.assertEquals(calls[0].getParam(0), 0)
        self.assertEquals(calls[1].getParam(0), 1)
        self.assertEquals(calls[2].getParam(0), 2)
        self.assertEquals(calls[0].getParam(1), app.nm.getNodeByUUID(uuid))
        self.assertEquals(calls[1].getParam(1), app.nm.getNodeByUUID(app.uuid))
        self.assertEquals(calls[2].getParam(1), app.nm.getNodeByUUID(app.uuid))
        self.assertEquals(calls[0].getParam(2), UP_TO_DATE_STATE)
        self.assertEquals(calls[1].getParam(2), DISCARDED_STATE)
        self.assertEquals(calls[2].getParam(2), OUT_OF_DATE_STATE)
        # replicator calls
        calls = self.app.replicator.mockGetNamedCalls('removePartition')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1)
        calls = self.app.replicator.mockGetNamedCalls('addPartition')
        # dm call
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), ptid2)
        self.assertEquals(calls[0].getParam(1), cells)
        self.assertEquals(len(calls), 1)

    def test_15_handleStartOperation(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleStartOperation,
            _msg_type=START_OPERATION,
        )

    def test_16_handleStopOperation1(self):
        # OperationFailure
        conn = Mock({ 'isServerConnection': False })
        packet = Packet(msg_id=1, msg_type=STOP_OPERATION)
        self.assertRaises(OperationFailure, self.operation.handleStopOperation, conn, packet)

    def test_16_handleStopOperation2(self):
        # server connection rejected
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleStopOperation,
            _msg_type=STOP_OPERATION,
            _listening=True,
        )

    def test_17_handleAskUnfinishedTransaction(self):
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskUnfinishedTransactions,
            _msg_type=ASK_UNFINISHED_TRANSACTIONS,
        )

    def test_18_handleAskTransactionInformation1(self):
        # transaction does not exists
        conn = Mock({ })
        packet = Packet(msg_id=1, msg_type=ASK_TRANSACTION_INFORMATION)
        self.operation.handleAskTransactionInformation(conn, packet, INVALID_TID)
        self.checkPacket(conn, packet_type=ERROR)

    def test_18_handleAskTransactionInformation2(self):
        # answer
        conn = Mock({ })
        packet = Packet(msg_id=1, msg_type=ASK_TRANSACTION_INFORMATION)
        dm = Mock({ "getTransaction": (INVALID_TID, 'user', 'desc', '', ), })
        self.app.dm = dm
        self.operation.handleAskTransactionInformation(conn, packet, INVALID_TID)
        self.checkPacket(conn, packet_type=ANSWER_TRANSACTION_INFORMATION)

    def test_19_handleAskObjectPresent(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskObjectPresent,
            _msg_type=ASK_OBJECT_PRESENT,
            oid=self.getNewUUID(),
            tid=INVALID_TID,
        )

    def test_20_handleDeleteTransaction(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleDeleteTransaction,
            _msg_type=DELETE_TRANSACTION,
            tid=INVALID_TID,
        )

    def test_21_handleCommitTransaction(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleCommitTransaction,
            _msg_type=COMMIT_TRANSACTION,
            tid=INVALID_TID,
        )

    def test_22_handleLockInformation1(self):
        # reject server connection 
        self.app.dm = Mock()
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleLockInformation,
            _msg_type=LOCK_INFORMATION,
            _listening=True,
            tid=INVALID_TID,
        )
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('storeTransaction')), 0)

    def test_22_handleLockInformation2(self):
        # load transaction informations
        conn = Mock({ 'isServerConnection': False, })
        self.app.dm = Mock({ })
        packet = Packet(msg_id=1, msg_type=LOCK_INFORMATION)
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        self.app.transaction_dict[INVALID_TID] = transaction
        self.operation.handleLockInformation(conn, packet, INVALID_TID)
        self.assertEquals(self.app.load_lock_dict[0], INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('storeTransaction')
        self.assertEquals(len(calls), 1)
        self.checkPacket(conn, packet_type=NOTIFY_INFORMATION_LOCKED)
        # transaction not in transaction_dict -> KeyError
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        conn = Mock({ 'isServerConnection': False, })
        self.operation.handleLockInformation(conn, packet, '\x01' * 8)
        self.checkPacket(conn, packet_type=NOTIFY_INFORMATION_LOCKED)

    def test_23_handleUnlockInformation1(self):
        # reject server connection
        self.app.dm = Mock()
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleUnlockInformation,
            _msg_type=UNLOCK_INFORMATION,
            _listening=True,
            tid=INVALID_TID,
        )
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('storeTransaction')), 0)

    def test_23_handleUnlockInformation2(self):
        # delete transaction informations
        conn = Mock({ 'isServerConnection': False, })
        self.app.dm = Mock({ })
        packet = Packet(msg_id=1, msg_type=LOCK_INFORMATION)
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        self.app.transaction_dict[INVALID_TID] = transaction
        self.app.load_lock_dict[0] = transaction
        self.app.store_lock_dict[0] = transaction
        self.operation.handleUnlockInformation(conn, packet, INVALID_TID)
        self.assertEquals(len(self.app.load_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)
        calls = self.app.dm.mockGetNamedCalls('finishTransaction')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), INVALID_TID)
        # transaction not in transaction_dict -> KeyError
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        conn = Mock({ 'isServerConnection': False, })
        self.operation.handleLockInformation(conn, packet, '\x01' * 8)
        self.checkPacket(conn, packet_type=NOTIFY_INFORMATION_LOCKED)

    def test_24_handleAskObject1(self):
        # delayed response
        conn = Mock({})
        self.app.dm = Mock()
        packet = Packet(msg_id=1, msg_type=ASK_OBJECT)
        self.app.load_lock_dict[INVALID_OID] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_handleAskObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OBJECT)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), INVALID_OID)
        self.assertEquals(calls[0].getParam(1), None)
        self.assertEquals(calls[0].getParam(2), None)
        self.checkPacket(conn, packet_type=ERROR)

    def test_24_handleAskObject3(self):
        # object found => answer
        self.app.dm = Mock({'getObject': ('', '', 0, 0, '', )})
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OBJECT)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.handleAskObject(conn, packet, 
            oid=INVALID_OID, 
            serial=INVALID_SERIAL, 
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkPacket(conn, packet_type=ANSWER_OBJECT)

    def test_25_handleAskTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_TIDS)
        self.operation.handleAskTIDs(conn, packet, 1, 1, None)
        self.checkPacket(conn, packet_type=ERROR)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)
        

    def test_25_handleAskTIDs2(self):
        # well case => answer
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_TIDS)
        self.app.num_partitions = 1
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.operation.handleAskTIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1)
        self.assertEquals(calls[0].getParam(1), 1)
        self.assertEquals(calls[0].getParam(2), 1)
        self.assertEquals(calls[0].getParam(3), [1, ])
        self.checkPacket(conn, packet_type=ANSWER_TIDS)

    def test_25_handleAskTIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_TIDS)
        self.app.num_partitions = 1
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getCellList': (cell, )})
        self.operation.handleAskTIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1)
        self.assertEquals(calls[0].getParam(1), 1)
        self.assertEquals(calls[0].getParam(2), 1)
        self.assertEquals(calls[0].getParam(3), [0, ])
        self.checkPacket(conn, packet_type=ANSWER_TIDS)

    def test_26_handleAskObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OBJECT_HISTORY)
        self.operation.handleAskObjectHistory(conn, packet, 1, 1, None)
        self.checkPacket(conn, packet_type=ERROR)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_handleAskObjectHistory2(self):
        # first case: empty history
        packet = Packet(msg_id=1, msg_type=ASK_OBJECT_HISTORY)
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.handleAskObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkPacket(conn, packet_type=ANSWER_OBJECT_HISTORY)
        # second case: not empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': [('', 0, ), ]})
        self.operation.handleAskObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkPacket(conn, packet_type=ANSWER_OBJECT_HISTORY)

    def test_27_handleAskStoreTransaction1(self):
        # no uuid => abort
        before = self.app.transaction_dict.items()[:]
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskStoreTransaction,
            _msg_type=ASK_STORE_TRANSACTION,
            tid=INVALID_TID, 
            user='', 
            desc='', 
            ext='', 
            oid_list=()
        )
        after = self.app.transaction_dict.items()
        self.assertEquals(before, after)

    def test_27_handleAskStoreTransaction2(self):
        # add transaction entry
        packet = Packet(msg_id=1, msg_type=ASK_STORE_TRANSACTION)
        conn = Mock({'getUUID': self.getNewUUID()})
        self.operation.handleAskStoreTransaction(conn, packet,
            INVALID_TID, '', '', '', ())
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertTrue(isinstance(t, TransactionInformation))
        self.assertEquals(t.getTransaction(), ((), '', '', ''))
        self.checkPacket(conn, packet_type=ANSWER_STORE_TRANSACTION)

    def test_28_handleAskStoreObject1(self):
        # no uuid => abort
        app = self.app
        oid = '\x01' * 8
        app.dm = Mock()
        l_before = len(app.event_queue)
        t_before = self.app.transaction_dict.items()[:]
        self.assertTrue(oid not in app.store_lock_dict)
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAskStoreObject,
            _msg_type=ASK_STORE_OBJECT,
            oid=oid,
            serial=INVALID_SERIAL,
            compression=0,
            checksum='',
            data='',
            tid=INVALID_TID
        )
        l_after = len(app.event_queue)
        self.assertEquals(l_before, l_after)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)
        t_after = self.app.transaction_dict.items()
        self.assertEquals(t_before, t_after)
        self.assertTrue(oid not in app.store_lock_dict)

    def test_28_handleAskStoreObject2(self):
        # locked => delayed response
        packet = Packet(msg_id=1, msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        oid = '\x02' * 8
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[oid] = tid1
        self.assertTrue(oid in self.app.store_lock_dict)
        t_before = self.app.transaction_dict.items()[:]
        self.operation.handleAskStoreObject(conn, packet, oid, 
            INVALID_SERIAL, 0, 0, '', tid2)
        self.assertEquals(len(self.app.event_queue), 1)
        t_after = self.app.transaction_dict.items()[:]
        self.assertEquals(t_before, t_after)
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)
        self.assertTrue(oid in self.app.store_lock_dict)

    def test_28_handleAskStoreObject3(self):
        # locked => unresolvable conflict => answer
        packet = Packet(msg_id=1, msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        tid1, tid2 = self.getTwoIDs()
        self.app.store_lock_dict[INVALID_OID] = tid2
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', tid1)
        self.checkPacket(conn, packet_type=ANSWER_STORE_OBJECT)
        self.assertEquals(self.app.store_lock_dict[INVALID_OID], tid2)
        # conflicting
        packet = conn.mockGetNamedCalls('addPacket')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
    
    def test_28_handleAskStoreObject4(self):
        # resolvable conflict => answer
        packet = Packet(msg_id=1, msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        self.app.dm = Mock({'getObjectHistory':((self.getNewUUID(), ), )})
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        self.checkPacket(conn, packet_type=ANSWER_STORE_OBJECT)
        self.assertEquals(self.app.store_lock_dict.get(INVALID_OID, None), None)
        # conflicting
        packet = conn.mockGetNamedCalls('addPacket')[0].getParam(0)
        self.assertTrue(unpack('!B8s8s', packet._body)[0])
        
    def test_28_handleAskStoreObject5(self):
        # no conflict => answer
        packet = Packet(msg_id=1, msg_type=ASK_STORE_OBJECT)
        conn = Mock({'getUUID': self.app.uuid})
        self.operation.handleAskStoreObject(conn, packet, INVALID_OID, 
            INVALID_SERIAL, 0, 0, '', INVALID_TID)
        t = self.app.transaction_dict.get(INVALID_TID, None)
        self.assertNotEquals(t, None)
        self.assertEquals(len(t.getObjectList()), 1)
        object = t.getObjectList()[0]
        self.assertEquals(object, (INVALID_OID, 0, 0, ''))
        self.checkPacket(conn, packet_type=ANSWER_STORE_OBJECT)
        # no conflict
        packet = conn.mockGetNamedCalls('addPacket')[0].getParam(0)
        self.assertFalse(unpack('!B8s8s', packet._body)[0])

    def test_29_handleAbortTransaction(self):
        # no uuid => abort
        before = self.app.transaction_dict.items()[:]
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAbortTransaction,
            _msg_type=ABORT_TRANSACTION,
            tid=INVALID_TID
        )
        after = self.app.transaction_dict.items()
        self.assertEquals(before, after)
        # remove transaction
        packet = Packet(msg_id=1, msg_type=ABORT_TRANSACTION)
        conn = Mock({'getUUID': self.app.uuid})
        transaction = Mock({ 'getObjectList': ((0, ), ), })
        self.called = False
        def called():
            self.called = True
        self.app.executeQueuedEvents = called
        self.app.load_lock_dict[0] = object()
        self.app.store_lock_dict[0] = object()
        self.app.transaction_dict[INVALID_TID] = transaction
        self.operation.handleAbortTransaction(conn, packet, INVALID_TID)
        self.assertTrue(self.called)
        self.assertEquals(len(self.app.load_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)
        self.assertEquals(len(self.app.store_lock_dict), 0)

    def test_30_handleAnswerLastIDs(self):
        # listening connection => unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAnswerLastIDs,
            _msg_type=ANSWER_LAST_IDS,
            _listening=True,
            loid=INVALID_OID,
            ltid=INVALID_TID,
            lptid=INVALID_TID,
        )
        # set critical TID on replicator
        conn = Mock()
        packet = Packet(msg_id=1, msg_type=ANSWER_LAST_IDS)
        self.app.replicator = Mock()
        self.operation.handleAnswerLastIDs(
            conn=conn,
            packet=packet,
            loid=INVALID_OID,
            ltid=INVALID_TID,
            lptid=INVALID_TID,
        )
        calls = self.app.replicator.mockGetNamedCalls('setCriticalTID')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), packet)
        self.assertEquals(calls[0].getParam(1), INVALID_TID)

    def test_31_handleAnswerUnfinishedTransactions(self):
        # unexpected packet
        self.checkHandleUnexpectedPacket(
            _call=self.operation.handleAnswerUnfinishedTransactions,
            _msg_type=ANSWER_UNFINISHED_TRANSACTIONS,
            _listening=True,
            tid_list=(INVALID_TID, ),
        )
        # set unfinished TID on replicator
        conn = Mock()
        packet = Packet(msg_id=1, msg_type=ANSWER_UNFINISHED_TRANSACTIONS)
        self.app.replicator = Mock()
        self.operation.handleAnswerUnfinishedTransactions(
            conn=conn,
            packet=packet,
            tid_list=(INVALID_TID, ),
        )
        calls = self.app.replicator.mockGetNamedCalls('setUnfinishedTIDList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), (INVALID_TID, ))

    def test_25_handleAskOIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OIDS)
        self.operation.handleAskOIDs(conn, packet, 1, 1, None)
        self.checkPacket(conn, packet_type=ERROR)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getOIDList')), 0)

    def test_25_handleAskOIDs2(self):
        # well case > answer OIDs
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OIDS)
        self.app.num_partitions = 1
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        self.operation.handleAskOIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1)
        self.assertEquals(calls[0].getParam(1), 1)
        self.assertEquals(calls[0].getParam(2), 1)
        self.assertEquals(calls[0].getParam(3), [1, ])
        self.checkPacket(conn, packet_type=ANSWER_OIDS)

    def test_25_handleAskOIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packet(msg_id=1, msg_type=ASK_OIDS)
        self.app.num_partitions = 1
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        self.app.pt = Mock({'getCellList': (cell, )})
        self.operation.handleAskOIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1)
        self.assertEquals(calls[0].getParam(1), 1)
        self.assertEquals(calls[0].getParam(2), 1)
        self.assertEquals(calls[0].getParam(3), [0, ])
        self.checkPacket(conn, packet_type=ANSWER_OIDS)


if __name__ == "__main__":
    unittest.main()
