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
import threading
from mock import Mock, ReturnValues
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.protocol import ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
     PING, PONG, ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, ANNOUNCE_PRIMARY_MASTER, \
     REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION, START_OPERATION, \
     STOP_OPERATION, ASK_LAST_IDS, ANSWER_LAST_IDS, ASK_PARTITION_TABLE, \
     ANSWER_PARTITION_TABLE, SEND_PARTITION_TABLE, NOTIFY_PARTITION_CHANGES, \
     ASK_UNFINISHED_TRANSACTIONS, ANSWER_UNFINISHED_TRANSACTIONS, \
     ASK_OBJECT_PRESENT, ANSWER_OBJECT_PRESENT, \
     DELETE_TRANSACTION, COMMIT_TRANSACTION, ASK_NEW_TID, ANSWER_NEW_TID, \
     FINISH_TRANSACTION, NOTIFY_TRANSACTION_FINISHED, LOCK_INFORMATION, \
     NOTIFY_INFORMATION_LOCKED, INVALIDATE_OBJECTS, UNLOCK_INFORMATION, \
     ASK_NEW_OIDS, ANSWER_NEW_OIDS, ASK_STORE_OBJECT, ANSWER_STORE_OBJECT, \
     ABORT_TRANSACTION, ASK_STORE_TRANSACTION, ANSWER_STORE_TRANSACTION, \
     ASK_OBJECT, ANSWER_OBJECT, ASK_TIDS, ANSWER_TIDS, ASK_TRANSACTION_INFORMATION, \
     ANSWER_TRANSACTION_INFORMATION, ASK_OBJECT_HISTORY, ANSWER_OBJECT_HISTORY, \
     ASK_OIDS, ANSWER_OIDS, INVALID_PTID, \
     NOT_READY_CODE, OID_NOT_FOUND_CODE, SERIAL_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
     PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE, \
     INTERNAL_ERROR_CODE, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import ElectionFailure
from neo.client.handler import BaseClientEventHandler, PrimaryBoostrapEventHandler, \
        PrimaryEventHandler, StorageBootstrapEventHandler, StorageEventHandler
from neo.node import StorageNode
from neo.util import dump

MARKER = []

class BaseClientEventHandlerTest(unittest.TestCase):

    def setUp(self):
        dispatcher = Mock({'getQueue': queue, 'connectToPrimaryMasterNode': None})
        self.handler = BaseClientEventHandler(dispatcher)

    def getConnection(self, uuid=None, port=10010, next_id=None, ip='127.0.0.1'):
        if uuid is None:
            uuid = self.getUUID()
        return Mock({'addPacket': None,
                     'getUUID': uuid,
                     'getAddress': (ip, port),
                     'getNextId': next_id,
                     'lock': None,
                     'unlock': None})


class ClientEventHandlerTest(unittest.TestCase):

    def setUp(self):
        # Silence all log messages
        logging.basicConfig(level=logging.CRITICAL + 1)

    def getConnection(self, uuid=None, port=10010, next_id=None, ip='127.0.0.1'):
        if uuid is None:
            uuid = self.getUUID()
        return Mock({'addPacket': None,
                     'getUUID': uuid,
                     'getAddress': (ip, port),
                     'getNextId': next_id,
                     'lock': None,
                     'unlock': None})

    def getUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def getDispatcher(self, queue=None):
      return Mock({'getQueue': queue, 'connectToPrimaryMasterNode': None})

    def test_ping(self):
        """
        Simplest test: check that a PING packet is answered by a PONG
        packet.
        """
        dispatcher = self.getDispatcher()
        client_handler = BaseClientEventHandler(None, dispatcher)
        conn = self.getConnection()
        client_handler.packetReceived(conn, protocol.ping())
        pong = conn.mockGetNamedCalls('answer')[0].getParam(0)
        self.assertTrue(isinstance(pong, Packet))
        self.assertEquals(pong.getType(), PONG)

    def _testInitialMasterWithMethod(self, method):
        class App:
            primary_master_node = None
        app = App()
        method(self.getDispatcher(), app, PrimaryBoostrapEventHandler)
        self.assertEqual(app.primary_master_node, -1)

    def _testMasterWithMethod(self, method, handler_class):
        uuid = self.getUUID()
        app = Mock({'connectToPrimaryMasterNode': None})
        app.primary_master_node = Mock({'getUUID': uuid})
        app.master_conn = Mock({'close': None, 'getUUID': uuid})
        dispatcher = self.getDispatcher()
        method(dispatcher, app, handler_class, uuid=uuid)
        # XXX: should connection closure be tested ? It's not implemented in all cases
        #self.assertEquals(len(App.master_conn.mockGetNamedCalls('close')), 1)
        #self.assertEquals(app.master_conn, None)
        #self.assertEquals(app.primary_master_node, None)
        self.assertEquals(len(app.mockGetNamedCalls('connectToPrimaryMasterNode')), 1)

    def _testStorageWithMethod(self, method, handler_class, state=TEMPORARILY_DOWN_STATE):
        storage_ip = '127.0.0.1'
        storage_port = 10011
        fake_storage_node_uuid = self.getUUID()
        fake_storage_node = Mock({'getUUID': fake_storage_node_uuid, 'getServer': (storage_ip, storage_port), 'getNodeType': STORAGE_NODE_TYPE})
        master_node_next_packet_id = 1
        class App:
            primary_master_node = Mock({'getUUID': self.getUUID()})
            nm = Mock({'getNodeByServer': fake_storage_node})
            cp = Mock({'removeConnection': None})
            master_conn = Mock({
                'addPacket': None,
                'getUUID': self.getUUID(),
                'getAddress': ('127.0.0.1', 10010),
                'getNextId': master_node_next_packet_id,
                'lock': None,
                'unlock': None
            })
        app = App()
        conn = self.getConnection(port=storage_port, ip=storage_ip)
        key_1 = (id(conn), 0)
        queue_1 = Mock({'put': None, '__hash__': 1})
        # Fake another Storage connection by adding 1 to id(conn)
        key_2 = (id(conn) + 1, 0)
        queue_2 = Mock({'put': None, '__hash__': 2})
        class Dispatcher:
            message_table = {key_1: queue_1,
                             key_2: queue_2}
        dispatcher = Dispatcher()
        method(dispatcher, app, handler_class, conn=conn)
        # Check that master was notified of the failure
        addPacket_call_list = app.master_conn.mockGetNamedCalls('notify')
          # Test sanity check
        self.assertEqual(len(addPacket_call_list), 1)
        node_status_packet = addPacket_call_list[0].getParam(0)
        self.assertTrue(isinstance(node_status_packet, Packet))
          # Test sanity check
        # the test below is disabled because the msg_id is now set by the connection
        #self.assertEquals(node_status_packet.getId(), master_node_next_packet_id)
        self.assertEquals(node_status_packet.getType(), NOTIFY_NODE_INFORMATION)
        self.assertEquals(node_status_packet.decode()[0],
                          [(STORAGE_NODE_TYPE, storage_ip, storage_port,
                            fake_storage_node_uuid, state), ]
        )
        # Check that failed connection got removed from connection pool
        removeConnection_call_list = app.cp.mockGetNamedCalls('removeConnection')
          # Test sanity check
        self.assertEqual(len(removeConnection_call_list), 1)
        self.assertTrue(removeConnection_call_list[0].getParam(0) is fake_storage_node)
        # Check that fake packet was put into queue_1, and none in queue_2.
        queue_1_put_call_list = queue_1.mockGetNamedCalls('put')
        self.assertEqual(len(queue_1_put_call_list), 1)
        self.assertEqual(queue_1_put_call_list[0].getParam(0), (conn, None))
        self.assertEqual(len(queue_2.mockGetNamedCalls('put')), 0)

    def _testConnectionFailed(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = handler_class(app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.connectionFailed(conn)

    def test_initialMasterConnectionFailed(self):
        self._testInitialMasterWithMethod(self._testConnectionFailed)

    def test_storageConnectionFailed(self):
        self._testStorageWithMethod(self._testConnectionFailed, 
                StorageBootstrapEventHandler)

    def _testConnectionClosed(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = handler_class(app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.connectionClosed(conn)

    def test_initialMasterConnectionClosed(self):
        self._testInitialMasterWithMethod(self._testConnectionClosed)

    def test_masterConnectionClosed(self):
        self._testMasterWithMethod(self._testConnectionClosed,
                PrimaryEventHandler)

    def test_storageConnectionClosed(self):
        self._testStorageWithMethod(self._testConnectionClosed, 
                StorageBootstrapEventHandler)
        self._testStorageWithMethod(self._testConnectionClosed, 
                StorageEventHandler)

    def _testTimeoutExpired(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = handler_class(app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.timeoutExpired(conn)

    def test_initialMasterTimeoutExpired(self):
        self._testInitialMasterWithMethod(self._testTimeoutExpired)

    def test_masterTimeoutExpired(self):
        self._testMasterWithMethod(self._testTimeoutExpired, PrimaryEventHandler)

    def test_storageTimeoutExpired(self):
        self._testStorageWithMethod(self._testTimeoutExpired, 
                StorageEventHandler)
        self._testStorageWithMethod(self._testTimeoutExpired, 
                StorageBootstrapEventHandler)

    def _testPeerBroken(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = handler_class(app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.peerBroken(conn)

    def test_initialMasterPeerBroken(self):
        self._testInitialMasterWithMethod(self._testPeerBroken)

    def test_masterPeerBroken(self):
        self._testMasterWithMethod(self._testPeerBroken, PrimaryEventHandler)

    def test_storagePeerBroken(self):
        self._testStorageWithMethod(self._testPeerBroken,
                StorageBootstrapEventHandler, state=BROKEN_STATE)
        self._testStorageWithMethod(self._testPeerBroken,
                StorageEventHandler, state=BROKEN_STATE)

    def test_notReady(self):
        app = Mock({'setNodeNotReady': None})
        dispatcher = self.getDispatcher()
        client_handler = PrimaryBoostrapEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleNotReady(conn, None, None)
        self.assertEquals(len(app.mockGetNamedCalls('setNodeNotReady')), 1)
        client_handler = StorageBootstrapEventHandler(app, dispatcher)
        client_handler.handleNotReady(conn, None, None)
        self.assertEquals(len(app.mockGetNamedCalls('setNodeNotReady')), 2)

    def test_clientAcceptNodeIdentification(self):
        class App:
            nm = Mock({'getNodeByServer': None})
            storage_node = None
            pt = None
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = PrimaryBoostrapEventHandler(app, dispatcher)
        conn = self.getConnection()
        uuid = self.getUUID()
        app.uuid = 'C' * 16
        client_handler.handleAcceptNodeIdentification(conn, None, CLIENT_NODE_TYPE,
                                                      uuid, '127.0.0.1', 10010,
                                                      0, 0, INVALID_UUID)
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 1)
        self.assertEquals(app.storage_node, None)
        self.assertEquals(app.pt, None)
        self.assertEquals(app.uuid, 'C' * 16)

    def test_masterAcceptNodeIdentification(self):
        node = Mock({'setUUID': None})
        class App:
            nm = Mock({'getNodeByServer': node})
            storage_node = None
            pt = None

            def getQueue(self):
                return None
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = PrimaryBoostrapEventHandler(app, dispatcher)
        conn = self.getConnection()
        uuid = self.getUUID()
        your_uuid = 'C' * 16
        app.uuid = INVALID_UUID
        client_handler.handleAcceptNodeIdentification(conn, None, MASTER_NODE_TYPE,
                                                      uuid, '127.0.0.1', 10010,
                                                      10, 2, your_uuid)
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('setUUID')), 1)
        setUUID_call_list = node.mockGetNamedCalls('setUUID')
        self.assertEquals(len(setUUID_call_list), 1)
        self.assertEquals(setUUID_call_list[0].getParam(0), uuid)
        self.assertEquals(app.storage_node, None)
        self.assertTrue(app.pt is not None)
        self.assertEquals(app.uuid, your_uuid)

    def test_storageAcceptNodeIdentification(self):
        node = Mock({'setUUID': None})
        class App:
            nm = Mock({'getNodeByServer': node})
            storage_node = None
            pt = None

            def getQueue(self):
                return None
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageBootstrapEventHandler(app, dispatcher)
        conn = self.getConnection()
        uuid = self.getUUID()
        app.uuid = 'C' * 16
        client_handler.handleAcceptNodeIdentification(conn, None, STORAGE_NODE_TYPE,
                                                      uuid, '127.0.0.1', 10010,
                                                      0, 0, INVALID_UUID)
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('setUUID')), 1)
        setUUID_call_list = node.mockGetNamedCalls('setUUID')
        self.assertEquals(len(setUUID_call_list), 1)
        self.assertEquals(setUUID_call_list[0].getParam(0), uuid)
        self.assertEquals(app.pt,  None)
        self.assertEquals(app.uuid, 'C' * 16)

    def _testHandleUnexpectedPacketCalledWithMedhod(self, client_handler, method, args=(), kw=()):
        # Monkey-patch handleUnexpectedPacket to check if it is called
        call_list = []
        def ClientHandler_handleUnexpectedPacket(self, conn, packet):
            call_list.append((conn, packet))
        original_handleUnexpectedPacket = client_handler.__class__.handleUnexpectedPacket
        client_handler.__class__.handleUnexpectedPacket = ClientHandler_handleUnexpectedPacket
        try:
            method(*args, **dict(kw))
        finally:
            # Restore original method
            client_handler.__class__.handleUnexpectedPacket = original_handleUnexpectedPacket
        # Check that packet was notified as unexpected.
        self.assertEquals(len(call_list), 1)
        # Return call_list in case caller wants to do extra checks on it.
        return call_list

    # Master node handler
    def test_initialAnswerPrimaryMaster(self):
        client_handler = PrimaryBoostrapEventHandler(None, self.getDispatcher())
        conn = Mock({'getUUID': None})
        call_list = self._testHandleUnexpectedPacketCalledWithMedhod(
            client_handler, client_handler.handleAnswerPrimaryMaster,
            args=(conn, None, 0, []))
        # Nothing else happened, or a raise would have happened (app is None,
        # and it's where things would happen)
        self.assertEquals(call_list[0], (conn, None))
        
    def test_nonMasterAnswerPrimaryMaster(self):
        for node_type in (CLIENT_NODE_TYPE, STORAGE_NODE_TYPE):
            node = Mock({'getNodeType': node_type})
            class App:
                nm = Mock({'getNodeByUUID': node, 'getNodeByServer': None, 'add': None})
            app = App()
            client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
            conn = self.getConnection()
            client_handler.handleAnswerPrimaryMaster(conn, None, 0, [])
            # Check that nothing happened
            self.assertEqual(len(app.nm.mockGetNamedCalls('getNodeByServer')), 0)
            self.assertEqual(len(app.nm.mockGetNamedCalls('add')), 0)

    def test_unknownNodeAnswerPrimaryMaster(self):
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': None, 'add': None})
            primary_master_node = None
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        test_master_list = [('127.0.0.1', 10010, self.getUUID())]
        client_handler.handleAnswerPrimaryMaster(conn, None, INVALID_UUID, test_master_list)
        # Test sanity check
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 1)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        # Check that yet-unknown master node got added
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        add_call_list = app.nm.mockGetNamedCalls('add')
        self.assertEqual(len(getNodeByServer_call_list), 1)
        self.assertEqual(len(add_call_list), 1)
        address, port, test_uuid = test_master_list[0]
        getNodeByServer_call = getNodeByServer_call_list[0]
        add_call = add_call_list[0]
        self.assertEquals((address, port), getNodeByServer_call.getParam(0))
        node_instance = add_call.getParam(0)
        self.assertEquals(test_uuid, node_instance.getUUID())
        # Check that primary master was not updated (it is not known yet,
        # hence INVALID_UUID in call).
        self.assertEquals(app.primary_master_node, None)

    def test_knownNodeUnknownUUIDNodeAnswerPrimaryMaster(self):
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': None, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': node, 'add': None})
            primary_master_node = None
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        test_node_uuid = self.getUUID()
        test_master_list = [('127.0.0.1', 10010, test_node_uuid)]
        client_handler.handleAnswerPrimaryMaster(conn, None, INVALID_UUID, test_master_list)
        # Test sanity checks
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 1)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        self.assertEqual(len(getNodeByServer_call_list), 1)
        self.assertEqual(getNodeByServer_call_list[0].getParam(0), test_master_list[0][:2])
        # Check that known master node did not get added
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        add_call_list = app.nm.mockGetNamedCalls('add')
        self.assertEqual(len(getNodeByServer_call_list), 1)
        self.assertEqual(len(add_call_list), 0)
        # Check that node UUID got updated
        setUUID_call_list = node.mockGetNamedCalls('setUUID')
        self.assertEqual(len(setUUID_call_list), 1)
        self.assertEqual(setUUID_call_list[0].getParam(0), test_node_uuid)
        # Check that primary master was not updated (it is not known yet,
        # hence INVALID_UUID in call).
        self.assertEquals(app.primary_master_node, None)

    def test_knownNodeKnownUUIDNodeAnswerPrimaryMaster(self):
        test_node_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_node_uuid, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': node, 'add': None})
            primary_master_node = None
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        test_master_list = [('127.0.0.1', 10010, test_node_uuid)]
        client_handler.handleAnswerPrimaryMaster(conn, None, INVALID_UUID, test_master_list)
        # Test sanity checks
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 1)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        self.assertEqual(len(getNodeByServer_call_list), 1)
        self.assertEqual(getNodeByServer_call_list[0].getParam(0), test_master_list[0][:2])
        # Check that known master node did not get added
        add_call_list = app.nm.mockGetNamedCalls('add')
        self.assertEqual(len(add_call_list), 0)
        # Check that node UUID was untouched
        # XXX: should we just check that there was either no call or a call
        # with same uuid, or enforce no call ? Here we enforce no call just
        # because it's what implementation does.
        setUUID_call_list = node.mockGetNamedCalls('setUUID')
        self.assertEqual(len(setUUID_call_list), 0)
        # Check that primary master was not updated (it is not known yet,
        # hence INVALID_UUID in call).
        self.assertEquals(app.primary_master_node, None)

    # TODO: test known node, known but different uuid (not detected in code,
    # desired behaviour unknown)

    def test_alreadyDifferentPrimaryAnswerPrimaryMaster(self):
        test_node_uuid = self.getUUID()
        test_primary_node_uuid = test_node_uuid
        while test_primary_node_uuid == test_node_uuid:
            test_primary_node_uuid = self.getUUID()
        test_primary_master_node = Mock({'getUUID': test_primary_node_uuid})
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_node_uuid, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': node, 'add': None})
            primary_master_node = test_primary_master_node
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        # If primary master is already set *and* is not given primary master
        # handle call raises.
        # XXX: is it acceptable for a handle call to raise without any proper fallback ?
        self.assertRaises(ElectionFailure, client_handler.handleAnswerPrimaryMaster,
                          conn, None, test_node_uuid, [])
        # Test sanity checks
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 1)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        self.assertEqual(len(getNodeByServer_call_list), 0)

    def test_alreadySamePrimaryAnswerPrimaryMaster(self):
        test_node_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_node_uuid, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': node, 'add': None})
            primary_master_node = node
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        client_handler.handleAnswerPrimaryMaster(conn, None, test_node_uuid, [])
        # Check that primary node is (still) node.
        self.assertTrue(app.primary_master_node is node)

    def test_unknownNewPrimaryAnswerPrimaryMaster(self):
        test_node_uuid = self.getUUID()
        test_primary_node_uuid = test_node_uuid
        while test_primary_node_uuid == test_node_uuid:
            test_primary_node_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_node_uuid, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': ReturnValues(node, None), 'getNodeByServer': node, 'add': None})
            primary_master_node = None
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        client_handler.handleAnswerPrimaryMaster(conn, None, test_primary_node_uuid, [])
        # Test sanity checks
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 2)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        self.assertEqual(getNodeByUUID_call_list[1].getParam(0), test_primary_node_uuid)
        # Check that primary node was not updated.
        self.assertTrue(app.primary_master_node is None)

    def test_AnswerPrimaryMaster(self):
        test_node_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_node_uuid, 'setUUID': None})
        class App:
            nm = Mock({'getNodeByUUID': node, 'getNodeByServer': node, 'add': None})
            primary_master_node = None
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        test_master_list = [('127.0.0.1', 10010, test_node_uuid)]
        client_handler.handleAnswerPrimaryMaster(conn, None, test_node_uuid, test_master_list)
        # Test sanity checks
        getNodeByUUID_call_list = app.nm.mockGetNamedCalls('getNodeByUUID')
        self.assertEqual(len(getNodeByUUID_call_list), 2)
        self.assertEqual(getNodeByUUID_call_list[0].getParam(0), conn.getUUID())
        self.assertEqual(getNodeByUUID_call_list[1].getParam(0), test_node_uuid)
        getNodeByServer_call_list = app.nm.mockGetNamedCalls('getNodeByServer')
        self.assertEqual(len(getNodeByServer_call_list), 1)
        self.assertEqual(getNodeByServer_call_list[0].getParam(0), test_master_list[0][:2])
        # Check that primary master was updated to known node
        self.assertTrue(app.primary_master_node is node)

    def test_initialSendPartitionTable(self):
        client_handler = PrimaryBoostrapEventHandler(None, self.getDispatcher())
        conn = Mock({'getUUID': None})
        call_list = self._testHandleUnexpectedPacketCalledWithMedhod(
            client_handler, client_handler.handleSendPartitionTable,
            args=(conn, None, None, None))
        self.assertEquals(call_list[0], (conn, None))

    def test_nonMasterSendPartitionTable(self):
        for node_type in (CLIENT_NODE_TYPE, STORAGE_NODE_TYPE):
            node = Mock({'getNodeType': node_type})
            class App:
                nm = Mock({'getNodeByUUID': node})
                pt = None
            app = App()
            client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
            conn = self.getConnection()
            client_handler.handleSendPartitionTable(conn, None, 0, [])
            # Check that nothing happened
            self.assertTrue(app.pt is None)

    def test_newSendPartitionTable(self):
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        test_ptid = 0
        class App:
            nm = Mock({'getNodeByUUID': node})
            pt = Mock({'clear': None})
            ptid = test_ptid
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        client_handler.handleSendPartitionTable(conn, None, test_ptid + 1, [])
        # Check that partition table got cleared and ptid got updated
        self.assertEquals(app.ptid, test_ptid + 1)
        self.assertEquals(len(app.pt.mockGetNamedCalls('clear')), 1)

    def test_unknownNodeSendPartitionTable(self):
        test_node = Mock({'getNodeType': MASTER_NODE_TYPE})
        test_ptid = 0
        class App:
            nm = Mock({'getNodeByUUID': ReturnValues(test_node, None), 'add': None})
            pt = Mock({'setCell': None})
            ptid = test_ptid
        test_storage_uuid = self.getUUID()
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        # TODO: use realistic values
        test_row_list = [(0, [(test_storage_uuid, 0)])]
        client_handler.handleSendPartitionTable(conn, None, test_ptid, test_row_list)
        # Check that node got created
        add_call_list = app.nm.mockGetNamedCalls('add')
        self.assertEquals(len(add_call_list), 1)
        created_node = add_call_list[0].getParam(0)
        self.assertEqual(created_node.getUUID(), test_storage_uuid)
        # Check that partition table cell got added
        setCell_call_list = app.pt.mockGetNamedCalls('setCell')
        self.assertEquals(len(setCell_call_list), 1)
        offset = setCell_call_list[0].getParam(0)
        node = setCell_call_list[0].getParam(1)
        state = setCell_call_list[0].getParam(2)
        self.assertEqual(offset, test_row_list[0][0])
        self.assertTrue(node is created_node)
        self.assertEqual(state, test_row_list[0][1][0][1])

    def test_knownNodeSendPartitionTable(self):
        test_node = Mock({'getNodeType': MASTER_NODE_TYPE})
        test_ptid = 0
        class App:
            nm = Mock({'getNodeByUUID': test_node, 'add': None})
            pt = Mock({'setCell': None})
            ptid = test_ptid
        test_storage_uuid = self.getUUID()
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        # TODO: use realistic values
        test_row_list = [(0, [(test_storage_uuid, 0)])]
        client_handler.handleSendPartitionTable(conn, None, test_ptid, test_row_list)
        # Check that node did not get created
        self.assertEquals(len(app.nm.mockGetNamedCalls('add')), 0)
        # Check that partition table cell got added
        setCell_call_list = app.pt.mockGetNamedCalls('setCell')
        self.assertEquals(len(setCell_call_list), 1)
        offset = setCell_call_list[0].getParam(0)
        node = setCell_call_list[0].getParam(1)
        state = setCell_call_list[0].getParam(2)
        self.assertEqual(offset, test_row_list[0][0])
        self.assertTrue(node is test_node)
        self.assertEqual(state, test_row_list[0][1][0][1])

    def test_initialNotifyNodeInformation(self):
        client_handler = PrimaryBoostrapEventHandler(None, self.getDispatcher())
        conn = Mock({'getUUID': None})
        call_list = self._testHandleUnexpectedPacketCalledWithMedhod(
            client_handler, client_handler.handleNotifyNodeInformation,
            args=(conn, None, None))
        self.assertEquals(call_list[0], (conn, None))

    def test_nonMasterNotifyNodeInformation(self):
        for node_type in (CLIENT_NODE_TYPE, STORAGE_NODE_TYPE):
            test_master_uuid = self.getUUID()
            node = Mock({'getNodeType': node_type})
            class App:
                nm = Mock({'getNodeByUUID': node})
            app = App()
            client_handler = PrimaryEventHandler(app, self.getDispatcher())
            conn = self.getConnection(uuid=test_master_uuid)
            client_handler.handleNotifyNodeInformation(conn, None, ())

    def test_nonIterableParameterRaisesNotifyNodeInformation(self):
        # XXX: this test is here for sanity self-check: it verifies the
        # assumption described in test_nonMasterNotifyNodeInformation
        # by making a valid call with a non-iterable parameter given as
        # node_list value.
        test_master_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        class App:
            nm = Mock({'getNodeByUUID': node})
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=test_master_uuid)
        self.assertRaises(TypeError, client_handler.handleNotifyNodeInformation,
            conn, None, None)

    def _testNotifyNodeInformation(self, test_node, getNodeByServer=None, getNodeByUUID=MARKER):
        invalid_uid_test_node = (test_node[0], test_node[1], test_node[2] + 1,
                                 INVALID_UUID, test_node[4])
        test_node_list = [test_node, invalid_uid_test_node]
        test_master_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        if getNodeByUUID is not MARKER:
            getNodeByUUID = ReturnValues(node, getNodeByUUID)
        class App:
            nm = Mock({'getNodeByUUID': getNodeByUUID,
                       'getNodeByServer': getNodeByServer,
                       'add': None,
                       'remove': None})
        app = App()
        #client_handler = ClientEventHandler(app, selClientEventHandlerf.getDispatcher())
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=test_master_uuid)
        client_handler.handleNotifyNodeInformation(conn, None, test_node_list)
        # Return nm so caller can check handler actions.
        return app.nm

    def test_unknownMasterNotifyNodeInformation(self):
        # first notify unknown master nodes
        uuid = self.getUUID()
        test_node = (MASTER_NODE_TYPE, '127.0.0.1', 10010, uuid,
                     RUNNING_STATE)
        nm = self._testNotifyNodeInformation(test_node, getNodeByUUID=None)
        # Check that two nodes got added (second is with INVALID_UUID)
        add_call_list = nm.mockGetNamedCalls('add')
        self.assertEqual(len(add_call_list), 2)
        added_node = add_call_list[0].getParam(0)
        self.assertEquals(added_node.getUUID(), uuid)
        added_node = add_call_list[1].getParam(0)
        self.assertEquals(added_node.getUUID(), None)

    def test_knownMasterNotifyNodeInformation(self):
        node = Mock({})
        uuid = self.getUUID()
        test_node = (MASTER_NODE_TYPE, '127.0.0.1', 10010, uuid,
                     RUNNING_STATE)
        nm = self._testNotifyNodeInformation(test_node, getNodeByServer=node,
                getNodeByUUID=node)
        # Check that no node got added
        self.assertEqual(len(nm.mockGetNamedCalls('add')), 0)
        add_call_list = node.mockGetNamedCalls('add')

    def test_unknownStorageNotifyNodeInformation(self):
        test_node = (STORAGE_NODE_TYPE, '127.0.0.1', 10010, self.getUUID(),
                     RUNNING_STATE)
        nm = self._testNotifyNodeInformation(test_node, getNodeByUUID=None)
        # Check that node got added
        add_call_list = nm.mockGetNamedCalls('add')
        self.assertEqual(len(add_call_list), 1)
        added_node = add_call_list[0].getParam(0)
        # XXX: this test does not check that node state got updated.
        # This is because there would be no way to tell the difference between
        # an updated state and default state if they are the same value (we
        # don't control node class/instance here)
        # Likewise for server address and node uuid.

    def test_knownStorageNotifyNodeInformation(self):
        node = Mock({'setState': None, 'setServer': None})
        test_node = (STORAGE_NODE_TYPE, '127.0.0.1', 10010, self.getUUID(),
                     RUNNING_STATE)
        nm = self._testNotifyNodeInformation(test_node, getNodeByUUID=node)
        # Check that no node got added
        self.assertEqual(len(nm.mockGetNamedCalls('add')), 0)
        # Check that server address got set
        setServer_call_list = node.mockGetNamedCalls('setServer')
        self.assertEqual(len(setServer_call_list), 1)
        self.assertEqual(setServer_call_list[0].getParam(0),
                         (test_node[1], test_node[2]))
        # Check node state has been updated
        setState_call_list = node.mockGetNamedCalls('setState')
        self.assertEqual(len(setState_call_list), 1)
        self.assertEqual(setState_call_list[0].getParam(0), test_node[4])

    def test_initialNotifyPartitionChanges(self):
        class App:
            nm = None
            pt = None
            ptid = INVALID_PTID
        app = App()
        client_handler = PrimaryBoostrapEventHandler(app, self.getDispatcher())
        conn = Mock({'getUUID': None})
        call_list = self._testHandleUnexpectedPacketCalledWithMedhod(
            client_handler, client_handler.handleNotifyPartitionChanges,
            args=(conn, None, None, None))
        self.assertEquals(call_list[0], (conn, None))

    def test_nonMasterNotifyPartitionChanges(self):
        for node_type in (CLIENT_NODE_TYPE, STORAGE_NODE_TYPE):
            test_master_uuid = self.getUUID()
            node = Mock({'getNodeType': node_type, 'getUUID': test_master_uuid})
            class App:
                nm = Mock({'getNodeByUUID': node})
                pt = None
                ptid = INVALID_PTID
                primary_master_node = node
            app = App()
            client_handler = PrimaryEventHandler(app, self.getDispatcher())
            conn = self.getConnection(uuid=test_master_uuid)
            client_handler.handleNotifyPartitionChanges(conn, None, 0, [])
            # Check that nothing happened
            self.assertTrue(app.pt is None)

    def test_noPrimaryMasterNotifyPartitionChanges(self):
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        class App:
            nm = Mock({'getNodeByUUID': node})
            pt = None
            ptid = INVALID_PTID
            primary_master_node = None
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection()
        client_handler.handleNotifyPartitionChanges(conn, None, 0, [])
        # Check that nothing happened
        self.assertTrue(app.pt is None)

    def test_nonPrimaryMasterNotifyPartitionChanges(self):
        test_master_uuid = self.getUUID()
        test_sender_uuid = test_master_uuid
        while test_sender_uuid == test_master_uuid:
            test_sender_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE})
        test_master_node = Mock({'getUUID': test_master_uuid})
        class App:
            nm = Mock({'getNodeByUUID': node})
            pt = None
            ptid = INVALID_PTID
            primary_master_node = test_master_node
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=test_sender_uuid)
        client_handler.handleNotifyPartitionChanges(conn, None, 0, [])
        # Check that nothing happened
        self.assertTrue(app.pt is None)

    def test_ignoreOutdatedPTIDNotifyPartitionChanges(self):
        test_master_uuid = self.getUUID()
        node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_master_uuid})
        test_ptid = 1
        class App:
            nm = Mock({'getNodeByUUID': node})
            pt = None
            primary_master_node = node
            ptid = test_ptid
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=test_master_uuid)
        client_handler.handleNotifyPartitionChanges(conn, None, test_ptid, [])
        # Check that nothing happened
        self.assertTrue(app.pt is None)
        self.assertEquals(app.ptid, test_ptid)

    def test_unknownNodeNotifyPartitionChanges(self):
        test_master_uuid = self.getUUID()
        test_node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': test_master_uuid})
        test_ptid = 1
        class App:
            nm = Mock({'getNodeByUUID': ReturnValues(None)})
            pt = Mock({'setCell': None})
            primary_master_node = test_node
            ptid = test_ptid
            uuid = None # XXX: Is it really needed ?
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=test_master_uuid)
        test_storage_uuid = self.getUUID()
        # TODO: use realistic values
        test_cell_list = [(0, test_storage_uuid, UP_TO_DATE_STATE)]
        client_handler.handleNotifyPartitionChanges(conn, None, test_ptid + 1, test_cell_list)
        # Check that a new node got added
        add_call_list = app.nm.mockGetNamedCalls('add')
        self.assertEqual(len(add_call_list), 1)
        added_node = add_call_list[0].getParam(0)
        # Check that partition got updated
        self.assertEqual(app.ptid, test_ptid + 1)
        setCell_call_list = app.pt.mockGetNamedCalls('setCell')
        self.assertEqual(len(setCell_call_list), 1)
        offset = setCell_call_list[0].getParam(0)
        node = setCell_call_list[0].getParam(1)
        state = setCell_call_list[0].getParam(2)
        self.assertEqual(offset, test_cell_list[0][0])
        self.assertTrue(node is added_node)
        self.assertEqual(state, test_cell_list[0][2])

    # TODO: confirm condition under which an unknown node should be added with a TEMPORARILY_DOWN_STATE (implementation is unclear)

    def test_knownNodeNotifyPartitionChanges(self):
        test_ptid = 1
        uuid1, uuid2 = self.getUUID(), self.getUUID()
        uuid3, uuid4 = self.getUUID(), self.getUUID()
        test_node = Mock({'getNodeType': MASTER_NODE_TYPE, 'getUUID': uuid1})
        class App:
            nm = Mock({'getNodeByUUID': ReturnValues(test_node, None, None, None), 'add': None})
            pt = Mock({'setCell': None})
            primary_master_node = test_node
            ptid = test_ptid
            uuid = uuid4
        app = App()
        client_handler = PrimaryEventHandler(app, self.getDispatcher())
        conn = self.getConnection(uuid=uuid1)
        test_cell_list = [
            (0, uuid1, UP_TO_DATE_STATE),
            (0, uuid2, DISCARDED_STATE),
            (0, uuid3, FEEDING_STATE),
            (0, uuid4, UP_TO_DATE_STATE),
        ]
        client_handler.handleNotifyPartitionChanges(conn, None, test_ptid + 1, test_cell_list)
        # Check that the three last node got added
        calls = app.nm.mockGetNamedCalls('add')
        self.assertEquals(len(calls), 3)
        self.assertEquals(calls[0].getParam(0).getUUID(), uuid2)
        self.assertEquals(calls[1].getParam(0).getUUID(), uuid3)
        self.assertEquals(calls[2].getParam(0).getUUID(), uuid4)
        self.assertEquals(calls[0].getParam(0).getState(), TEMPORARILY_DOWN_STATE)
        self.assertEquals(calls[1].getParam(0).getState(), TEMPORARILY_DOWN_STATE)
        # check two are dropped from the pt
        calls = app.pt.mockGetNamedCalls('dropNode')
        self.assertEquals(len(calls), 2)
        self.assertEquals(calls[0].getParam(0).getUUID(), uuid2)
        self.assertEquals(calls[1].getParam(0).getUUID(), uuid3)
        # and the others are updated
        self.assertEqual(app.ptid, test_ptid + 1)
        calls = app.pt.mockGetNamedCalls('setCell')
        self.assertEqual(len(calls), 2)
        self.assertEquals(calls[0].getParam(1).getUUID(), uuid1)
        self.assertEquals(calls[1].getParam(1).getUUID(), uuid4)

    def test_AnswerNewTID(self):
        app = Mock({'setTID': None})
        dispatcher = self.getDispatcher()
        client_handler = PrimaryEventHandler(app, dispatcher)
        conn = self.getConnection()
        test_tid = 1
        client_handler.handleAnswerNewTID(conn, None, test_tid)
        setTID_call_list = app.mockGetNamedCalls('setTID')
        self.assertEquals(len(setTID_call_list), 1)
        self.assertEquals(setTID_call_list[0].getParam(0), test_tid)

    def test_NotifyTransactionFinished(self):
        test_tid = 1
        app = Mock({'getTID': test_tid, 'setTransactionFinished': None})
        dispatcher = self.getDispatcher()
        client_handler = PrimaryEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleNotifyTransactionFinished(conn, None, test_tid)
        self.assertEquals(len(app.mockGetNamedCalls('setTransactionFinished')), 1)
        # TODO: decide what to do when non-current transaction is notified as finished, and test that behaviour

    def test_InvalidateObjects(self):
        class App:
            def _cache_lock_acquire(self):
                pass

            def _cache_lock_release(self):
                pass

            def registerDB(self, db, limit):
                self.db = db

            def getDB(self):
                return self.db

            mq_cache = Mock({'__delitem__': None})
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        test_tid = 1
        test_oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        test_db = Mock({'invalidate': None})
        app.registerDB(test_db, None)
        client_handler.handleInvalidateObjects(conn, None, test_oid_list[:], test_tid)
        # 'invalidate' is called just once
        db = app.getDB()
        self.assertTrue(db is test_db)
        invalidate_call_list = db.mockGetNamedCalls('invalidate')
        self.assertEquals(len(invalidate_call_list), 1)
        invalidate_call = invalidate_call_list[0]
        invalidate_tid = invalidate_call.getParam(0)
        self.assertEquals(invalidate_tid, test_tid)
        invalidate_oid_dict = invalidate_call.getParam(1)
        self.assertEquals(len(invalidate_oid_dict), len(test_oid_list))
        self.assertEquals(set(invalidate_oid_dict), set(test_oid_list))
        self.assertEquals(set(invalidate_oid_dict.itervalues()), set([test_tid]))
        # '__delitem__' is called once per invalidated object
        delitem_call_list = app.mq_cache.mockGetNamedCalls('__delitem__')
        self.assertEquals(len(delitem_call_list), len(test_oid_list))
        oid_list = [x.getParam(0) for x in delitem_call_list]
        self.assertEquals(set(oid_list), set(test_oid_list))

    def test_AnswerNewOIDs(self):
        class App:
            new_oid_list = []
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = PrimaryEventHandler(app, dispatcher)
        conn = self.getConnection()
        test_oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        client_handler.handleAnswerNewOIDs(conn, None, test_oid_list[:])
        self.assertEquals(set(app.new_oid_list), set(test_oid_list))

    def test_StopOperation(self):
        raise NotImplementedError

    # Storage node handler

    def test_AnswerObject(self):
        class FakeLocal:
            asked_object = ()
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        # TODO: use realistic values
        test_object_data = ('\x00\x00\x00\x00\x00\x00\x00\x01', 0, 0, 0, 0, 'test')
        client_handler.handleAnswerObject(conn, None, *test_object_data)
        self.assertEquals(app.local_var.asked_object, test_object_data)

    def _testAnswerStoreObject(self, app, conflicting, oid, serial):
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleAnswerStoreObject(conn, None, conflicting, oid, serial)

    def test_conflictingAnswerStoreObject(self):
        class App:
            local_var = threading.local()
        app = App()
        app.local_var.object_stored = (0, 0)
        test_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
        test_serial = 1
        self._testAnswerStoreObject(app, 1, test_oid, test_serial)
        self.assertEqual(app.local_var.object_stored, (-1, test_serial))

    def test_AnswerStoreObject(self):
        class App:
            local_var = threading.local()
        app = App()
        app.local_var.object_stored = (0, 0)
        test_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
        test_serial = 1
        self._testAnswerStoreObject(app, 0, test_oid, test_serial)
        self.assertEqual(app.local_var.object_stored, (test_oid, test_serial))

    def test_AnswerStoreTransaction(self):
        test_tid = 10
        app = Mock({'getTID': test_tid, 'setTransactionVoted': None})
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleAnswerStoreTransaction(conn, None, test_tid)
        self.assertEquals(len(app.mockGetNamedCalls('setTransactionVoted')), 1)
        # TODO: test handleAnswerObject with test_tid not matching app.tid (not handled in program)

    def test_AnswerTransactionInformation(self):
        class FakeLocal:
            txn_info = {}
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        tid = '\x00\x00\x00\x00\x00\x00\x00\x01' # TODO: use a more realistic tid
        user = 'bar'
        desc = 'foo'
        ext = 0 # XXX: unused in implementation
        oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        client_handler.handleAnswerTransactionInformation(conn, None, tid, user, desc, ext, oid_list[:])
        stored_dict = app.local_var.txn_info
        # TODO: test 'time' value ?
        self.assertEquals(stored_dict['user_name'], user)
        self.assertEquals(stored_dict['description'], desc)
        self.assertEquals(stored_dict['id'], tid)
        self.assertEquals(stored_dict['oids'], oid_list)

    def test_AnswerObjectHistory(self):
        class FakeLocal:
            history = (0, [])
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        test_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
        # TODO: use realistic values
        test_history_list = [(1, 2), (3, 4)]
        client_handler.handleAnswerObjectHistory(conn, None, test_oid, test_history_list[:])
        oid, history = app.local_var.history
        self.assertEquals(oid, test_oid)
        self.assertEquals(len(history), len(test_history_list))
        self.assertEquals(set(history), set(test_history_list))

    def test_OidNotFound(self):
        class FakeLocal:
            asked_object = 0
            history = 0
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleOidNotFound(conn, None, None)
        self.assertEquals(app.local_var.asked_object, -1)
        self.assertEquals(app.local_var.history, -1)

    def test_TidNotFound(self):
        class FakeLocal:
            txn_info = 0
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        client_handler.handleTidNotFound(conn, None, None)
        self.assertEquals(app.local_var.txn_info, -1)

    def test_AnswerTIDs(self):
        class FakeLocal:
            node_tids = {}
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageEventHandler(app, dispatcher)
        conn = self.getConnection()
        test_tid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        client_handler.handleAnswerTIDs(conn, None, test_tid_list[:])
        stored_tid_list = []
        for tid_list in app.local_var.node_tids.itervalues():
            stored_tid_list.extend(tid_list)
        self.assertEquals(len(stored_tid_list), len(test_tid_list))
        self.assertEquals(set(stored_tid_list), set(test_tid_list))

if __name__ == '__main__':
    unittest.main()

