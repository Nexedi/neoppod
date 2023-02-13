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

import os, unittest
from .. import getTempDirectory, DB_PREFIX
from .testStorageDBTests import StorageDBTests
from neo.storage.database.sqlite import SQLiteDatabaseManager

class StorageSQLiteTests(StorageDBTests):

    def _test_lockDatabase_open(self):
        db = os.path.join(getTempDirectory(), DB_PREFIX + '0.sqlite')
        return SQLiteDatabaseManager(db)

    def _getDB(self, reset=False):
        db = SQLiteDatabaseManager(':memory:')
        db.setup(reset, True)
        return db

    def test_lockDatabase(self):
        super(StorageSQLiteTests, self).test_lockDatabase()
        # No lock on temporary databases.
        db = self._getDB()
        self._getDB().close()
        db.close()

del StorageDBTests

if __name__ == "__main__":
    unittest.main()
