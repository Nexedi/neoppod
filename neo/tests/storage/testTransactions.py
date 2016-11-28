#
# Copyright (C) 2010-2016  Nexedi SA
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
from neo.storage.transactions import Transaction, TransactionManager


class TransactionTests(NeoUnitTestBase):

    def testLock(self):
        txn = Transaction(self.getClientUUID(), self.getNextTID())
        self.assertFalse(txn.isLocked())
        txn.lock()
        self.assertTrue(txn.isLocked())
        # disallow lock more than once
        self.assertRaises(AssertionError, txn.lock)

class TransactionManagerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.app = Mock()
        # no history
        self.app.dm = Mock({'getObjectHistory': []})
        self.app.pt = Mock({'isAssigned': True})
        self.app.em = Mock({'setTimeout': None})
        self.manager = TransactionManager(self.app)

    def register(self, uuid, ttid):
        self.manager.register(Mock({'getUUID': uuid}), ttid)

    def test_updateObjectDataForPack(self):
        ram_serial = self.getNextTID()
        oid = self.getOID(1)
        orig_serial = self.getNextTID()
        uuid = self.getClientUUID()
        locking_serial = self.getNextTID()
        other_serial = self.getNextTID()
        new_serial = self.getNextTID()
        checksum = "2" * 20
        self.register(uuid, locking_serial)
        # Object not known, nothing happens
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), None)
        self.manager.updateObjectDataForPack(oid, orig_serial, None, checksum)
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), None)
        self.manager.abort(locking_serial, even_if_locked=True)
        # Object known, but doesn't point at orig_serial, it is not updated
        self.register(uuid, locking_serial)
        self.manager.storeObject(locking_serial, ram_serial, oid, 0, "3" * 20,
            'bar', None)
        holdData = self.app.dm.mockGetNamedCalls('holdData')
        self.assertEqual(holdData.pop(0).params, ("3" * 20, 'bar', 0))
        orig_object = self.manager.getObjectFromTransaction(locking_serial,
            oid)
        self.manager.updateObjectDataForPack(oid, orig_serial, None, checksum)
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), orig_object)
        self.manager.abort(locking_serial, even_if_locked=True)

        self.register(uuid, locking_serial)
        self.manager.storeObject(locking_serial, ram_serial, oid, None, None,
            None, other_serial)
        orig_object = self.manager.getObjectFromTransaction(locking_serial,
            oid)
        self.manager.updateObjectDataForPack(oid, orig_serial, None, checksum)
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), orig_object)
        self.manager.abort(locking_serial, even_if_locked=True)
        # Object known and points at undone data it gets updated
        self.register(uuid, locking_serial)
        self.manager.storeObject(locking_serial, ram_serial, oid, None, None,
            None, orig_serial)
        self.manager.updateObjectDataForPack(oid, orig_serial, new_serial,
            checksum)
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), (oid, None, new_serial))
        self.manager.abort(locking_serial, even_if_locked=True)

        self.register(uuid, locking_serial)
        self.manager.storeObject(locking_serial, ram_serial, oid, None, None,
            None, orig_serial)
        self.manager.updateObjectDataForPack(oid, orig_serial, None, checksum)
        self.assertEqual(holdData.pop(0).params, (checksum,))
        self.assertEqual(self.manager.getObjectFromTransaction(locking_serial,
            oid), (oid, checksum, None))
        self.manager.abort(locking_serial, even_if_locked=True)
        self.assertFalse(holdData)

if __name__ == "__main__":
    unittest.main()
