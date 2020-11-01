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
from contextlib import contextmanager
from MySQLdb import NotSupportedError, OperationalError, ProgrammingError
from MySQLdb.constants.CR import SERVER_GONE_ERROR
from MySQLdb.constants.ER import UNKNOWN_STORAGE_ENGINE
from ..mock import Mock
from neo.lib.protocol import ZERO_OID
from neo.lib.util import p64
from .. import DB_PREFIX, DB_SOCKET, DB_USER, Patch
from .testStorageDBTests import StorageDBTests
from neo.storage.database import DatabaseFailure
from neo.storage.database.mysqldb import MySQLDatabaseManager


class ServerGone(object):

    @contextmanager
    def __new__(cls, db):
        self = object.__new__(cls)
        with Patch(db, conn=self) as self._p:
            yield self._p

    def query(self, *args):
        self._p.revert()
        raise OperationalError(SERVER_GONE_ERROR, 'this is a test')


class StorageMySQLdbTests(StorageDBTests):

    engine = None

    def _test_lockDatabase_open(self):
        self.prepareDatabase(number=1, prefix=DB_PREFIX)
        database = '%s@%s0%s' % (DB_USER, DB_PREFIX, DB_SOCKET)
        return MySQLDatabaseManager(database, self.engine)

    def getDB(self, reset=0):
        db = self._test_lockDatabase_open()
        self.assertEqual(db.db, DB_PREFIX + '0')
        self.assertEqual(db.user, DB_USER)
        try:
            db.setup(reset, True)
        except NotSupportedError as m:
            code, m = m.args
            if code != UNKNOWN_STORAGE_ENGINE:
                raise
            raise unittest.SkipTest(m)
        return db

    def test_query1(self):
        # fake result object
        from array import array
        result_object = Mock({
            "num_rows": 1,
            "fetch_row": ((1, 2, array('b', (1, 2, ))), ),
        })
        # expected formatted result
        expected_result = (
            (1, 2, '\x01\x02', ),
        )
        self.db.conn = Mock({ 'store_result': result_object })
        result = self.db.query('SELECT ')
        self.assertEqual(result, expected_result)
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs('SELECT ')

    def test_query2(self):
        with ServerGone(self.db) as p:
            self.assertRaises(ProgrammingError, self.db.query, 'QUERY')
            self.assertFalse(p.applied)

    def test_query3(self):
        # OperationalError > raise DatabaseFailure exception
        class FakeConn(object):
            def close(self):
                pass
            def query(*args):
                raise OperationalError(-1, 'this is a test')
        self.db.conn = FakeConn()
        self.assertRaises(DatabaseFailure, self.db.query, 'QUERY')

    def test_escape(self):
        self.assertEqual(self.db.escape('a"b'), 'a\\"b')
        self.assertEqual(self.db.escape("a'b"), "a\\'b")

    def test_max_allowed_packet(self):
        EXTRA = 2
        # Check EXTRA
        x = "SELECT '%s'" % ('x' * (self.db._max_allowed_packet - 11))
        assert len(x) + EXTRA == self.db._max_allowed_packet
        self.assertRaises(DatabaseFailure, self.db.query, x + ' ')
        self.db.query(x)
        # Reconnection cleared the cache of the config table,
        # so fill it again with required values before we patch query().
        self.db._getPartition
        # Check MySQLDatabaseManager._max_allowed_packet
        query_list = []
        self.db.query = lambda query: query_list.append(EXTRA + len(query))
        self.assertEqual(2, max(len(self.db.escape(chr(x)))
                                for x in xrange(256)))
        self.assertEqual(2, len(self.db.escape('\0')))
        self.db.storeData('\0' * 20, ZERO_OID, '\0' * (2**24-1), 0)
        size, = query_list
        max_allowed = self.db.__class__._max_allowed_packet
        self.assertTrue(max_allowed - 1024 < size <= max_allowed, size)
        # Check storeTransaction
        for count, max_allowed_packet in (7, 64), (6, 65), (1, 215):
            self.db._max_allowed_packet = max_allowed_packet
            del query_list[:]
            self.db.storeTransaction(p64(0),
                ((p64(1<<i),1234,None) for i in xrange(10)), None)
            self.assertEqual(max(query_list), max_allowed_packet)
            self.assertEqual(len(query_list), count)


class StorageMySQLdbRocksDBTests(StorageMySQLdbTests):

    engine = "RocksDB"
    test_lockDatabase = None


class StorageMySQLdbTokuDBTests(StorageMySQLdbTests):

    engine = "TokuDB"
    test_lockDatabase = None

del StorageDBTests

if __name__ == "__main__":
    unittest.main()
