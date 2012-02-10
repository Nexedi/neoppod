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
from neo.lib.protocol import NodeTypes, LockState
from neo.client.handlers.storage import StorageBootstrapHandler, \
       StorageAnswersHandler
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError
from ZODB.POSException import ConflictError
from neo.lib.exception import NodeNotReady
from ZODB.TimeStamp import TimeStamp

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
        self.assertRaises(NodeNotReady, self.handler.notReady, conn, 'message')

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
        self.handler = StorageAnswersHandler(self.app)

    def getConnection(self):
        return self.getFakeConnection()

    def _checkHandlerData(self, ref):
        calls = self.app.mockGetNamedCalls('setHandlerData')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(ref)

    def test_answerObject(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        the_object = (oid, tid1, tid2, 0, '', 'DATA', None)
        self.handler.answerObject(conn, *the_object)
        self._checkHandlerData(the_object[:-1])

    def _getAnswerStoreObjectHandler(self, object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict):
        app = Mock({
            'getHandlerData': {
                'object_stored_counter_dict': object_stored_counter_dict,
                'conflict_serial_dict': conflict_serial_dict,
                'resolved_conflict_serial_dict': resolved_conflict_serial_dict,
            }
        })
        return StorageAnswersHandler(app)

    def test_answerStoreObject_1(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        # conflict
        object_stored_counter_dict = {oid: {}}
        conflict_serial_dict = {}
        resolved_conflict_serial_dict = {}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(conflict_serial_dict[oid], set([tid, ]))
        self.assertEqual(object_stored_counter_dict[oid], {})
        self.assertFalse(oid in resolved_conflict_serial_dict)
        # object was already accepted by another storage, raise
        handler = self._getAnswerStoreObjectHandler({oid: {tid: set([1])}}, {}, {})
        self.assertRaises(NEOStorageError, handler.answerStoreObject,
            conn, 1, oid, tid)

    def test_answerStoreObject_2(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # resolution-pending conflict
        object_stored_counter_dict = {oid: {}}
        conflict_serial_dict = {oid: set([tid, ])}
        resolved_conflict_serial_dict = {}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(conflict_serial_dict[oid], set([tid, ]))
        self.assertFalse(oid in resolved_conflict_serial_dict)
        self.assertEqual(object_stored_counter_dict[oid], {})
        # object was already accepted by another storage, raise
        handler = self._getAnswerStoreObjectHandler({oid: {tid: set([1])}},
            {oid: set([tid, ])}, {})
        self.assertRaises(NEOStorageError, handler.answerStoreObject,
            conn, 1, oid, tid)
        # detected conflict is different, don't raise
        self._getAnswerStoreObjectHandler({oid: {}}, {oid: set([tid, ])}, {},
            ).answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_3(self):
        conn = self.getConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # already-resolved conflict
        # This case happens if a storage is answering a store action for which
        # any other storage already answered (with same conflict) and any other
        # storage accepted the resolved object.
        object_stored_counter_dict = {oid: {tid_2: 1}}
        conflict_serial_dict = {}
        resolved_conflict_serial_dict = {oid: set([tid, ])}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertFalse(oid in conflict_serial_dict)
        self.assertEqual(resolved_conflict_serial_dict[oid],
            set([tid, ]))
        self.assertEqual(object_stored_counter_dict[oid], {tid_2: 1})
        # detected conflict is different, don't raise
        self._getAnswerStoreObjectHandler({oid: {tid: 1}}, {},
            {oid: set([tid, ])}).answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_4(self):
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid)
        oid = self.getOID(0)
        tid = self.getNextTID()
        # no conflict
        object_stored_counter_dict = {oid: {}}
        conflict_serial_dict = {}
        resolved_conflict_serial_dict = {}
        h = self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict)
        h.app.getHandlerData()['cache_dict'] = {oid: None}
        h.answerStoreObject(conn, 0, oid, tid)
        self.assertFalse(oid in conflict_serial_dict)
        self.assertFalse(oid in resolved_conflict_serial_dict)
        self.assertEqual(object_stored_counter_dict[oid], {tid: set([uuid])})

    def test_answerTransactionInformation(self):
        conn = self.getConnection()
        tid = self.getNextTID()
        user = 'USER'
        desc = 'DESC'
        ext = 'EXT'
        packed = False
        oid_list = [self.getOID(0), self.getOID(1)]
        self.handler.answerTransactionInformation(conn, tid, user, desc, ext,
            packed, oid_list)
        self._checkHandlerData(({
            'time': TimeStamp(tid).timeTime(),
            'user_name': user,
            'description': desc,
            'id': tid,
            'oids': oid_list,
            'packed': packed,
        }, ext))

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
        tid_set = set()
        app = Mock({
            'getHandlerData': tid_set,
        })
        handler = StorageAnswersHandler(app)

        handler.answerTIDs(conn, tid_list)
        self.assertEqual(tid_set, set(tid_list))

    def test_answerObjectUndoSerial(self):
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid)
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        undo_dict = {}
        app = Mock({
            'getHandlerData': undo_dict,
        })
        handler = StorageAnswersHandler(app)
        handler.answerObjectUndoSerial(conn, {oid1: [tid0, tid1]})
        self.assertEqual(undo_dict, {oid1: [tid0, tid1]})
        handler.answerObjectUndoSerial(conn, {oid2: [tid2, tid3]})
        self.assertEqual(undo_dict, {
            oid1: [tid0, tid1],
            oid2: [tid2, tid3],
        })

if __name__ == '__main__':
    unittest.main()

