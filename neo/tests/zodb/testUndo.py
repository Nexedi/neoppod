#
# Copyright (C) 2009-2019  Nexedi SA
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
from ZODB.tests.StorageTestBase import StorageTestBase
from ZODB.tests.TransactionalUndoStorage import TransactionalUndoStorage
from ZODB.tests.ConflictResolution import ConflictResolvingTransUndoStorage

from neo.client.app import Application as ClientApplication
from .. import expectedFailure, Patch
from . import ZODBTestCase

class UndoTests(ZODBTestCase, StorageTestBase, TransactionalUndoStorage,
        ConflictResolvingTransUndoStorage):

    checkTransactionalUndoAfterPack = expectedFailure()(
        TransactionalUndoStorage.checkTransactionalUndoAfterPack)

class AltUndoTests(UndoTests):
    """
    These tests covers the beginning of an alternate implementation of undo,
    as described by the IDEA comment in the undo method of client's app.
    More precisely, they check that the protocol keeps support for data=None
    in AskStoreObject when cells are readable.
    """

    __patch = Patch(ClientApplication, _store=
        lambda orig, self, txn_context, oid, serial, data, data_serial=None:
            orig(self, txn_context, oid, serial,
                 None if data_serial else data, data_serial))

    def setUp(self):
        super(AltUndoTests, self).setUp()
        self.__patch.apply()

    def _tearDown(self, success):
        self.__patch.revert()
        super(AltUndoTests, self)._tearDown(success)

if __name__ == "__main__":
    suite = unittest.TestSuite((
        unittest.makeSuite(UndoTests, 'check'),
        unittest.makeSuite(AltUndoTests, 'check'),
        ))
    unittest.main(defaultTest='suite')
