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
from neo.tests import NeoUnitTestBase
from neo.protocol import NodeTypes, LockState
from neo.client.handlers.storage import StorageBootstrapHandler, \
       StorageAnswersHandler
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError
from ZODB.POSException import ConflictError

MARKER = []

class StorageBootstrapHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.app = Mock()
        self.handler = StorageBootstrapHandler(self.app)

    def getConnection(self):
        return self.getFakeConnection()

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


class StorageAnswerHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.app = Mock()
        self.app.local_var = Mock()
        self.handler = StorageAnswersHandler(self.app)

    def getConnection(self):
        return self.getFakeConnection()

    def test_answerObject(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        the_object = (oid, tid1, tid2, 0, '', 'DATA', None)
        self.app.local_var.asked_object = None
        self.handler.answerObject(conn, *the_object)
        self.assertEqual(self.app.local_var.asked_object, the_object[:-1])
        # Check handler raises on non-None data_serial.
        the_object = (oid, tid1, tid2, 0, '', 'DATA', self.getNextTID())
        self.app.local_var.asked_object = None
        self.assertRaises(NEOStorageError, self.handler.answerObject, conn,
            *the_object)

    def test_answerStoreObject_1(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        # conflict
        local_var = self.app.local_var
        local_var.object_stored_counter_dict = {oid: {}}
        local_var.conflict_serial_dict = {}
        local_var.resolved_conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(local_var.conflict_serial_dict[oid], set([tid, ]))
        self.assertEqual(local_var.object_stored_counter_dict[oid], {})
        self.assertFalse(oid in local_var.resolved_conflict_serial_dict)
        # object was already accepted by another storage, raise
        local_var.object_stored_counter_dict = {oid: {tid: 1}}
        local_var.conflict_serial_dict = {}
        local_var.resolved_conflict_serial_dict = {}
        self.assertRaises(NEOStorageError, self.handler.answerStoreObject,
            conn, 1, oid, tid)

    def test_answerStoreObject_2(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # resolution-pending conflict
        local_var = self.app.local_var
        local_var.object_stored_counter_dict = {oid: {}}
        local_var.conflict_serial_dict = {oid: set([tid, ])}
        local_var.resolved_conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(local_var.conflict_serial_dict[oid], set([tid, ]))
        self.assertFalse(oid in local_var.resolved_conflict_serial_dict)
        self.assertEqual(local_var.object_stored_counter_dict[oid], {})
        # object was already accepted by another storage, raise
        local_var.object_stored_counter_dict = {oid: {tid: 1}}
        local_var.conflict_serial_dict = {oid: set([tid, ])}
        local_var.resolved_conflict_serial_dict = {}
        self.assertRaises(NEOStorageError, self.handler.answerStoreObject,
            conn, 1, oid, tid)
        # detected conflict is different, don't raise
        local_var.object_stored_counter_dict = {oid: {}}
        local_var.conflict_serial_dict = {oid: set([tid, ])}
        local_var.resolved_conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_3(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # already-resolved conflict
        # This case happens if a storage is answering a store action for which
        # any other storage already answered (with same conflict) and any other
        # storage accepted the resolved object.
        local_var = self.app.local_var
        local_var.object_stored_counter_dict = {oid: {tid_2: 1}}
        local_var.conflict_serial_dict = {}
        local_var.resolved_conflict_serial_dict = {oid: set([tid, ])}
        self.handler.answerStoreObject(conn, 1, oid, tid)
        self.assertFalse(oid in local_var.conflict_serial_dict)
        self.assertEqual(local_var.resolved_conflict_serial_dict[oid],
            set([tid, ]))
        self.assertEqual(local_var.object_stored_counter_dict[oid], {tid_2: 1})
        # detected conflict is different, don't raise
        local_var.object_stored_counter_dict = {oid: {tid: 1}}
        local_var.conflict_serial_dict = {}
        local_var.resolved_conflict_serial_dict = {oid: set([tid, ])}
        self.handler.answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_4(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        # no conflict
        local_var = self.app.local_var
        local_var.object_stored_counter_dict = {oid: {}}
        local_var.conflict_serial_dict = {}
        local_var.resolved_conflict_serial_dict = {}
        self.handler.answerStoreObject(conn, 0, oid, tid)
        self.assertFalse(oid in local_var.conflict_serial_dict)
        self.assertFalse(oid in local_var.resolved_conflict_serial_dict)
        self.assertEqual(local_var.object_stored_counter_dict[oid], {tid: 1})

    def test_answerStoreTransaction(self):
        conn = self.getConnection()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        # wrong tid
        app = Mock({'getTID': tid1})
        handler = StorageAnswersHandler(app=app)
        self.assertRaises(NEOStorageError,
            handler.answerStoreTransaction, conn,
            tid2)
        # good tid
        app = Mock({'getTID': tid2})
        handler = StorageAnswersHandler(app=app)
        handler.answerStoreTransaction(conn, tid2)

    def test_answerTransactionInformation(self):
        conn = self.getConnection()
        tid = self.getNextTID()
        user = 'USER'
        desc = 'DESC'
        ext = 'EXT'
        oid_list = [self.getOID(0), self.getOID(1)]
        self.app.local_var.txn_info = None
        self.handler.answerTransactionInformation(conn, tid, user, desc, ext,
            False, oid_list)
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
        self.assertRaises(NEOStorageNotFoundError, self.handler.oidNotFound,
            conn, 'message')

    def test_oidDoesNotExist(self):
        conn = self.getConnection()
        self.assertRaises(NEOStorageDoesNotExistError,
            self.handler.oidDoesNotExist, conn, 'message')
        
    def test_tidNotFound(self):
        conn = self.getConnection()
        self.assertRaises(NEOStorageNotFoundError, self.handler.tidNotFound,
            conn, 'message')

    def test_answerTIDs(self):
        uuid = self.getNewUUID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        tid_list = [tid1, tid2]
        conn = self.getFakeConnection(uuid=uuid)
        self.app.local_var.node_tids = {}
        self.handler.answerTIDs(conn, tid_list)
        self.assertTrue(uuid in self.app.local_var.node_tids)
        self.assertEqual(self.app.local_var.node_tids[uuid], tid_list)

    def test_answerObjectUndoSerial(self):
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid)
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        self.app.local_var.undo_object_tid_dict = undo_dict = {
            oid1: [tid0, tid1],
        }
        self.handler.answerObjectUndoSerial(conn, {
            oid2: [tid2, tid3],
        })
        self.assertEqual(undo_dict, {
            oid1: [tid0, tid1],
            oid2: [tid2, tid3],
        })

    def test_answerHasLock(self):
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid)
        oid = self.getOID(0)

        self.assertRaises(ConflictError, self.handler.answerHasLock, conn, oid,
            LockState.GRANTED_TO_OTHER)
        # XXX: Just check that this doesn't raise for the moment.
        self.handler.answerHasLock(conn, oid, LockState.GRANTED)
        # TODO: Test LockState.NOT_LOCKED case when implemented.

if __name__ == '__main__':
    unittest.main()

