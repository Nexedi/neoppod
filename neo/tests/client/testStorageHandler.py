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
from mock import Mock
from neo.tests import NeoTestBase
from neo.protocol import NodeTypes
from neo.client.handlers.storage import StorageBootstrapHandler, \
       StorageAnswersHandler

MARKER = []

class StorageBootstrapHandlerTests(NeoTestBase):

    def setUp(self):
        self.app = Mock()
        self.handler = StorageBootstrapHandler(self.app)

    def getConnection(self):
        return Mock({'getAddress': ('fake-conn', 0)})

    def test_notReady(self):
        conn = self.getConnection()
        self.handler.notReady(conn, 'message')
        calls = self.app.mockGetNamedCalls('setNodeNotReady')
        self.assertEqual(len(calls), 1)

    def test_acceptIdentification1(self):
        """ Not a storage node """
        uuid = self.getNewUUID()
        conn = self.getConnection()
        conn = self.getConnection()
        node = Mock()
        self.app.nm = Mock({'getByAddress': node})
        self.handler.acceptIdentification(conn, NodeTypes.CLIENT, uuid,
            10, 0, None)
        self.checkClosed(conn)

    def test_acceptIdentification2(self):
        uuid = self.getNewUUID()
        conn = self.getConnection()
        node = Mock()
        self.app.nm = Mock({'getByAddress': node})
        self.handler.acceptIdentification(conn, NodeTypes.STORAGE, uuid,
            10, 0, None)
        self.checkUUIDSet(node, uuid)
        self.checkUUIDSet(conn, uuid)


class StorageAnswerHandlerTests(NeoTestBase):

    def setUp(self):
        self.app = Mock()
        self.app.local_var = Mock()
        self.handler = StorageAnswersHandler(self.app)

    def getConnection(self):
        return Mock({'getAddress': ('fake-conn', 0)})

    def test_answerObject(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        the_object = (oid, tid1, tid2, 0, '', 'DATA')
        self.app.local_var.asked_object = None
        self.handler.answerObject(conn, *the_object)
        self.assertEqual(self.app.local_var.asked_object, the_object)

    def test_answerStoreObject(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        # conflict
        local_var = self.app.local_var
        local_var.object_stored_counter_dict = {oid: 0}
        local_var.conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(local_var.conflict_serial_dict[oid], tid)
        self.assertFalse(local_var.object_stored_counter_dict[oid], 0)
        # no conflict
        local_var.object_stored_counter_dict = {oid: 0}
        local_var.conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 0, oid, tid)
        self.assertFalse(oid in local_var.conflict_serial_dict)
        self.assertEqual(local_var.object_stored_counter_dict[oid], 1)

    def test_answerStoreTransaction(self):
        conn = self.getConnection()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        # wrong tid
        app = Mock({'getTID': tid1})
        handler = StorageAnswersHandler(app=app)
        self.checkProtocolErrorRaised(handler.answerStoreTransaction, conn, 
            tid2)
        # good tid
        app = Mock({'getTID': tid2})
        handler = StorageAnswersHandler(app=app)
        handler.answerStoreTransaction(conn, tid2)
        calls = app.mockGetNamedCalls('setTransactionVoted')
        self.assertEqual(len(calls), 1)

    def test_answerTransactionInformation(self):
        conn = self.getConnection()
        tid = self.getNextTID()
        user = 'USER'
        desc = 'DESC'
        ext = 'EXT'
        oid_list = [self.getOID(0), self.getOID(1)]
        self.app.local_var.txn_info = None
        self.handler.answerTransactionInformation(conn, tid, user, desc, ext,
            oid_list)
        txn_info = self.app.local_var.txn_info
        self.assertTrue(isinstance(txn_info, dict))
        self.assertEqual(txn_info['user_name'], user)
        self.assertEqual(txn_info['description'], desc)
        self.assertEqual(txn_info['id'], tid)
        self.assertEqual(txn_info['oids'], oid_list)

    def test_answerObjectHistory(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        history_list = []
        self.app.local_var.history = None
        self.handler.answerObjectHistory(conn, oid, history_list)
        self.assertEqual(self.app.local_var.history, (oid, history_list))

    def test_oidNotFound(self):
        conn = self.getConnection()
        self.handler.oidNotFound(conn, 'message')
        self.assertEqual(self.app.local_var.asked_object, -1)
        self.assertEqual(self.app.local_var.history, -1)
        
    def test_tidNotFound(self):
        conn = self.getConnection()
        self.handler.tidNotFound(conn, 'message')
        self.assertEqual(self.app.local_var.txn_info, -1)

    def test_answerTIDs(self):
        uuid = self.getNewUUID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        tid_list = [tid1, tid2]
        conn = Mock({'getUUID': uuid})
        self.app.local_var.node_tids = {}
        self.handler.answerTIDs(conn, tid_list)
        self.assertTrue(uuid in self.app.local_var.node_tids)
        self.assertEqual(self.app.local_var.node_tids[uuid], tid_list)


if __name__ == '__main__':
    unittest.main()

