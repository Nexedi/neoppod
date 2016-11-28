#
# Copyright (C) 2009-2016  Nexedi SA
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

class StorageAnswerHandlerTests(NeoUnitTestBase):

    def setUp(self):
        super(StorageAnswerHandlerTests, self).setUp()
        self.app = Mock()
        self.handler = StorageAnswersHandler(self.app)

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

    def test_tidNotFound(self):
        conn = self.getFakeConnection()
        self.assertRaises(NEOStorageNotFoundError, self.handler.tidNotFound,
            conn, 'message')

if __name__ == '__main__':
    unittest.main()

