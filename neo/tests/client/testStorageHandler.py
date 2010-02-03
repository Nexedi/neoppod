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
import threading
from mock import Mock, ReturnValues
from neo.tests import NeoTestBase
from neo import protocol
from neo.pt import PartitionTable
from neo.protocol import UnexpectedPacketError, INVALID_UUID, INVALID_PTID
from neo.protocol import NodeTypes, NodeStates, CellStates, Packets
from neo.client.handlers import BaseHandler
from neo.client.handlers.master import PrimaryBootstrapHandler
from neo.client.handlers.master import PrimaryNotificationsHandler, PrimaryAnswersHandler
from neo.client.handlers.storage import StorageBootstrapHandler, StorageAnswersHandler

MARKER = []

class StorageBootstrapHandlerTests(NeoTestBase):

    def setUp(self):
        pass


class StorageAnswerHandlerTests(NeoTestBase):

    def setUp(self):
        pass



class _(object):

    def getConnection(self, uuid=None, port=10010, next_id=None, ip='127.0.0.1'):
        if uuid is None:
            uuid = self.getNewUUID()
        return Mock({'_addPacket': None,
                     'getUUID': uuid,
                     'getAddress': (ip, port),
                     'getNextId': next_id,
                     'getPeerId': 0,
                     'lock': None,
                     'unlock': None})

    def getDispatcher(self, queue=None):
        return Mock({'getQueue': queue, 'connectToPrimaryNode': None})

    def buildHandler(self, handler_class, app, dispatcher):
        # some handlers do not accept the second argument
        try:
            return handler_class(app, dispatcher)
        except TypeError:
            return handler_class(app)

    def test_ping(self):
        """
        Simplest test: check that a PING packet is answered by a PONG
        packet.
        """
        dispatcher = self.getDispatcher()
        client_handler = BaseHandler(None, dispatcher)
        conn = self.getConnection()
        packet = protocol.Ping()
        client_handler.packetReceived(conn, packet)
        self.checkAnswerPacket(conn, protocol.PONG)

    def _testInitialMasterWithMethod(self, method):
        class App:
            primary_master_node = None
            trying_master_node = 1
        app = App()
        method(self.getDispatcher(), app, PrimaryBootstrapHandler)
        self.assertEqual(app.primary_master_node, None)

    def _testMasterWithMethod(self, method, handler_class):
        uuid = self.getNewUUID()
        app = Mock({'connectToPrimaryNode': None})
        app.primary_master_node = Mock({'getUUID': uuid})
        app.master_conn = Mock({'close': None, 'getUUID': uuid, 'getAddress': ('127.0.0.1', 10000)})
        dispatcher = self.getDispatcher()
        method(dispatcher, app, handler_class, uuid=uuid, conn=app.master_conn)
        # XXX: should connection closure be tested ? It's not implemented in all cases
        #self.assertEquals(len(App.master_conn.mockGetNamedCalls('close')), 1)
        #self.assertEquals(app.master_conn, None)
        #self.assertEquals(app.primary_master_node, None)

    def _testStorageWithMethod(self, method, handler_class, state=NodeStates.TEMPORARILY_DOWN):
        storage_ip = '127.0.0.1'
        storage_port = 10011
        fake_storage_node_uuid = self.getNewUUID()
        fake_storage_node = Mock({
            'getUUID': fake_storage_node_uuid,
            'getAddress': (storage_ip, storage_port),
            'getType': NodeTypes.STORAGE
        })
        master_node_next_packet_id = 1
        class App:
            primary_master_node = Mock({'getUUID': self.getNewUUID()})
            nm = Mock({'getByAddress': fake_storage_node})
            cp = Mock({'removeConnection': None})
            master_conn = Mock({
                '_addPacket': None,
                'getUUID': self.getNewUUID(),
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
        # The master should be notified, but this is done in app.py
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
        client_handler = handler_class(app)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.connectionFailed(conn)

    def test_initialMasterConnectionFailed(self):
        self._testInitialMasterWithMethod(self._testConnectionFailed)

    def test_storageConnectionFailed(self):
        self._testStorageWithMethod(self._testConnectionFailed,
                StorageBootstrapHandler)

    def _testConnectionClosed(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = self.buildHandler(handler_class, app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.connectionClosed(conn)

    def test_initialMasterConnectionClosed(self):
        self._testInitialMasterWithMethod(self._testConnectionClosed)

    def test_masterConnectionClosed(self):
        self._testMasterWithMethod(self._testConnectionClosed,
                PrimaryNotificationsHandler)

    def test_storageConnectionClosed(self):
        self._testStorageWithMethod(self._testConnectionClosed,
                StorageBootstrapHandler)

    def _testTimeoutExpired(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = self.buildHandler(handler_class, app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.timeoutExpired(conn)

    def test_initialMasterTimeoutExpired(self):
        self._testInitialMasterWithMethod(self._testTimeoutExpired)

    def test_masterTimeoutExpired(self):
        self._testMasterWithMethod(self._testTimeoutExpired, PrimaryNotificationsHandler)

    def test_storageTimeoutExpired(self):
        self._testStorageWithMethod(self._testTimeoutExpired,
                StorageBootstrapHandler)

    def _testPeerBroken(self, dispatcher, app, handler_class, uuid=None, conn=None):
        client_handler = self.buildHandler(handler_class, app, dispatcher)
        if conn is None:
            conn = self.getConnection(uuid=uuid)
        client_handler.peerBroken(conn)

    def test_initialMasterPeerBroken(self):
        self._testInitialMasterWithMethod(self._testPeerBroken)

    def test_masterPeerBroken(self):
        self._testMasterWithMethod(self._testPeerBroken, PrimaryNotificationsHandler)

    def test_storagePeerBroken(self):
        self._testStorageWithMethod(self._testPeerBroken,
                StorageBootstrapHandler, state=NodeStates.BROKEN)

    def test_notReady(self):
        app = Mock({'setNodeNotReady': None})
        dispatcher = self.getDispatcher()
        conn = self.getConnection()
        client_handler = StorageBootstrapHandler(app)
        client_handler.notReady(conn, None)
        self.assertEquals(len(app.mockGetNamedCalls('setNodeNotReady')), 1)

    def test_clientAcceptIdentification(self):
        class App:
            nm = Mock({'getByAddress': None})
            storage_node = None
            pt = None
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = PrimaryBootstrapHandler(app)
        conn = self.getConnection()
        uuid = self.getNewUUID()
        app.uuid = 'C' * 16
        client_handler.acceptIdentification(conn, NodeTypes.CLIENT,
            uuid, 0, 0, INVALID_UUID)
        self.checkClosed(conn)
        self.assertEquals(app.storage_node, None)
        self.assertEquals(app.pt, None)
        self.assertEquals(app.uuid, 'C' * 16)

    def test_masterAcceptIdentification(self):
        node = Mock({'setUUID': None})
        class FakeLocal:
            from Queue import Queue
            queue = Queue()
        class App:
            nm = Mock({'getByAddress': node})
            storage_node = None
            pt = None
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = PrimaryBootstrapHandler(app)
        conn = self.getConnection()
        uuid = self.getNewUUID()
        your_uuid = 'C' * 16
        app.uuid = INVALID_UUID
        client_handler.acceptIdentification(conn, NodeTypes.MASTER,
            uuid, 10, 2, your_uuid)
        self.checkNotClosed(conn)
        self.checkUUIDSet(conn, uuid)
        self.assertEquals(app.storage_node, None)
        self.assertTrue(app.pt is not None)
        self.assertEquals(app.uuid, your_uuid)

    def test_storageAcceptIdentification(self):
        node = Mock({'setUUID': None})
        class App:
            nm = Mock({'getByAddress': node})
            storage_node = None
            pt = None
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageBootstrapHandler(app)
        conn = self.getConnection()
        uuid = self.getNewUUID()
        app.uuid = 'C' * 16
        client_handler.acceptIdentification(conn, NodeTypes.STORAGE,
            uuid, 0, 0, INVALID_UUID)
        self.checkNotClosed(conn)
        self.checkUUIDSet(conn, uuid)
        self.assertEquals(app.pt,  None)
        self.assertEquals(app.uuid, 'C' * 16)

    def _testHandleUnexpectedPacketCalledWithMedhod(self, method, args=(), kw=()):
        self.assertRaises(UnexpectedPacketError, method, *args, **dict(kw))


    # Storage node handler

    def test_AnswerObject(self):
        class FakeLocal:
            asked_object = ()
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        # XXX: use realistic values
        test_object_data = ('\x00\x00\x00\x00\x00\x00\x00\x01', 0, 0, 0, 0, 'test')
        client_handler.answerObject(conn, *test_object_data)
        self.assertEquals(app.local_var.asked_object, test_object_data)

    def _testAnswerStoreObject(self, app, conflicting, oid, serial):
        dispatcher = self.getDispatcher()
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        client_handler.answerStoreObject(conn, conflicting, oid, serial)

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
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        client_handler.answerStoreTransaction(conn, test_tid)
        self.assertEquals(len(app.mockGetNamedCalls('setTransactionVoted')), 1)
        # XXX: test answerObject with test_tid not matching app.tid (not handled in program)

    def test_AnswerTransactionInformation(self):
        class FakeLocal:
            txn_info = {}
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        tid = self.getNextTID()
        user = 'bar'
        desc = 'foo'
        ext = 0 # XXX: unused in implementation
        oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        client_handler.answerTransactionInformation(conn, tid, user, desc, ext, oid_list[:])
        stored_dict = app.local_var.txn_info
        # XXX: test 'time' value ?
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
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        test_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
        # XXX: use realistic values
        test_history_list = [(1, 2), (3, 4)]
        client_handler.answerObjectHistory(conn, test_oid, test_history_list[:])
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
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        client_handler.oidNotFound(conn, None)
        self.assertEquals(app.local_var.asked_object, -1)
        self.assertEquals(app.local_var.history, -1)

    def test_TidNotFound(self):
        class FakeLocal:
            txn_info = 0
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        client_handler.tidNotFound(conn, None)
        self.assertEquals(app.local_var.txn_info, -1)

    def test_AnswerTIDs(self):
        class FakeLocal:
            node_tids = {}
        class App:
            local_var = FakeLocal()
        app = App()
        dispatcher = self.getDispatcher()
        client_handler = StorageAnswersHandler(app)
        conn = self.getConnection()
        test_tid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        client_handler.answerTIDs(conn, test_tid_list[:])
        stored_tid_list = []
        for tid_list in app.local_var.node_tids.itervalues():
            stored_tid_list.extend(tid_list)
        self.assertEquals(len(stored_tid_list), len(test_tid_list))
        self.assertEquals(set(stored_tid_list), set(test_tid_list))

if __name__ == '__main__':
    unittest.main()

