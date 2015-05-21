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
from ZODB.tests import testZODB
import ZODB

from . import ZODBTestCase

class NEOZODBTests(ZODBTestCase, testZODB.ZODBTests):

    def setUp(self):
        super(NEOZODBTests, self).setUp()
        self._db = ZODB.DB(self._storage)

    def _tearDown(self, success):
        self._db.close()
        super(NEOZODBTests, self)._tearDown(success)

    def checkMultipleUndoInOneTransaction(self):
        # XXX: Upstream test accesses a persistent object outside a transaction
        #      (it should call transaction.begin() after the last commit)
        #      so disable our Connection.afterCompletion optimization.
        #      This should really be discussed on zodb-dev ML.
        from ZODB.Connection import Connection
        afterCompletion = Connection.__dict__['afterCompletion']
        try:
            Connection.afterCompletion = Connection.__dict__['newTransaction']
            super(NEOZODBTests, self).checkMultipleUndoInOneTransaction()
        finally:
            Connection.afterCompletion = afterCompletion

if __name__ == "__main__":
    suite = unittest.makeSuite(NEOZODBTests, 'check')
    unittest.main(defaultTest='suite')
