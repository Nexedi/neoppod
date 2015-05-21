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
from mock import Mock
from .. import NeoUnitTestBase
from neo.client.handlers.storage import StorageAnswersHandler
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError
from ZODB.TimeStamp import TimeStamp

class StorageAnswerHandlerTests(NeoUnitTestBase):

    def setUp(self):
        super(StorageAnswerHandlerTests, self).setUp()
        self.app = Mock()
        self.handler = StorageAnswersHandler(self.app)

    def _checkHandlerData(self, ref):
        calls = self.app.mockGetNamedCalls('setHandlerData')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(ref)

    def test_answerObject(self):
        conn = self.getFakeConnection()
        oid = self.getOID(0)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        the_object = (oid, tid1, tid2, 0, '', 'DATA', None)
        self.handler.answerObject(conn, *the_object)
        self._checkHandlerData(the_object[1:])

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
        conn = self.getFakeConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        # conflict
        object_stored_counter_dict = {oid: {}}
        conflict_serial_dict = {}
        resolved_conflict_serial_dict = {}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(conflict_serial_dict[oid], {tid})
        self.assertEqual(object_stored_counter_dict[oid], {})
        self.assertFalse(oid in resolved_conflict_serial_dict)
        # object was already accepted by another storage, raise
        handler = self._getAnswerStoreObjectHandler({oid: {tid: {1}}}, {}, {})
        self.assertRaises(NEOStorageError, handler.answerStoreObject,
            conn, 1, oid, tid)

    def test_answerStoreObject_2(self):
        conn = self.getFakeConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # resolution-pending conflict
        object_stored_counter_dict = {oid: {}}
        conflict_serial_dict = {oid: {tid}}
        resolved_conflict_serial_dict = {}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertEqual(conflict_serial_dict[oid], {tid})
        self.assertFalse(oid in resolved_conflict_serial_dict)
        self.assertEqual(object_stored_counter_dict[oid], {})
        # object was already accepted by another storage, raise
        handler = self._getAnswerStoreObjectHandler({oid: {tid: {1}}},
            {oid: {tid}}, {})
        self.assertRaises(NEOStorageError, handler.answerStoreObject,
            conn, 1, oid, tid)
        # detected conflict is different, don't raise
        self._getAnswerStoreObjectHandler({oid: {}}, {oid: {tid}}, {},
            ).answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_3(self):
        conn = self.getFakeConnection()
        oid = self.getOID(0)
        tid = self.getNextTID()
        tid_2 = self.getNextTID()
        # already-resolved conflict
        # This case happens if a storage is answering a store action for which
        # any other storage already answered (with same conflict) and any other
        # storage accepted the resolved object.
        object_stored_counter_dict = {oid: {tid_2: 1}}
        conflict_serial_dict = {}
        resolved_conflict_serial_dict = {oid: {tid}}
        self._getAnswerStoreObjectHandler(object_stored_counter_dict,
            conflict_serial_dict, resolved_conflict_serial_dict,
            ).answerStoreObject(conn, 1, oid, tid)
        self.assertFalse(oid in conflict_serial_dict)
        self.assertEqual(resolved_conflict_serial_dict[oid], {tid})
        self.assertEqual(object_stored_counter_dict[oid], {tid_2: 1})
        # detected conflict is different, don't raise
        self._getAnswerStoreObjectHandler({oid: {tid: 1}}, {},
            {oid: {tid}}).answerStoreObject(conn, 1, oid, tid_2)

    def test_answerStoreObject_4(self):
        uuid = self.getStorageUUID()
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
        self.assertEqual(object_stored_counter_dict[oid], {tid: {uuid}})

    def test_answerTransactionInformation(self):
        conn = self.getFakeConnection()
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
        conn = self.getFakeConnection()
        self.assertRaises(NEOStorageNotFoundError, self.handler.oidNotFound,
            conn, 'message')

    def test_oidDoesNotExist(self):
        conn = self.getFakeConnection()
        self.assertRaises(NEOStorageDoesNotExistError,
            self.handler.oidDoesNotExist, conn, 'message')

    def test_tidNotFound(self):
        conn = self.getFakeConnection()
        self.assertRaises(NEOStorageNotFoundError, self.handler.tidNotFound,
            conn, 'message')

    def test_answerTIDs(self):
        uuid = self.getStorageUUID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        tid_list = [tid1, tid2]
        conn = self.getFakeConnection(uuid=uuid)
        tid_set = set()
        StorageAnswersHandler(Mock()).answerTIDs(conn, tid_list, tid_set)
        self.assertEqual(tid_set, set(tid_list))

    def test_answerObjectUndoSerial(self):
        uuid = self.getStorageUUID()
        conn = self.getFakeConnection(uuid=uuid)
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        undo_dict = {}
        handler = StorageAnswersHandler(Mock())
        handler.answerObjectUndoSerial(conn, {oid1: [tid0, tid1]}, undo_dict)
        self.assertEqual(undo_dict, {oid1: [tid0, tid1]})
        handler.answerObjectUndoSerial(conn, {oid2: [tid2, tid3]}, undo_dict)
        self.assertEqual(undo_dict, {
            oid1: [tid0, tid1],
            oid2: [tid2, tid3],
        })

if __name__ == '__main__':
    unittest.main()

