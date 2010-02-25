#
# Copyright (C) 2006-2010  Nexedi SA
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

import MySQLdb
from MySQLdb import OperationalError
from MySQLdb.constants.CR import SERVER_GONE_ERROR, SERVER_LOST
from neo import logging
from array import array
import string

from neo.storage.database import DatabaseManager
from neo.exception import DatabaseFailure
from neo.protocol import CellStates
from neo import util

LOG_QUERIES = False

class MySQLDatabaseManager(DatabaseManager):
    """This class manages a database on MySQL."""

    def __init__(self, database):
        super(MySQLDatabaseManager, self).__init__()
        self.user, self.passwd, self.db = self._parse(database)
        self.conn = None
        self._connect()

    def _parse(self, database):
        """ Get the database credentials (username, password, database) """
        # expected pattern : [user[:password]@]database
        username = None
        password = None
        if '@' in database:
            (username, database) = database.split('@')
            if ':' in username:
                (username, password) = username.split(':')
        return (username, password, database)

    def close(self):
        self.conn.close()

    def _connect(self):
        kwd = {'db' : self.db, 'user' : self.user}
        if self.passwd is not None:
            kwd['passwd'] = self.passwd
        logging.info('connecting to MySQL on the database %s with user %s',
                     self.db, self.user)
        self.conn = MySQLdb.connect(**kwd)
        self.conn.autocommit(False)

    def _begin(self):
        self.query("""BEGIN""")

    def _commit(self):
        self.conn.commit()

    def _rollback(self):
        self.conn.rollback()

    def query(self, query):
        """Query data from a database."""
        conn = self.conn
        try:
            if LOG_QUERIES:
                printable_char_list = []
                for c in query.split('\n', 1)[0][:70]:
                    if c not in string.printable or c in '\t\x0b\x0c\r':
                        c = '\\x%02x' % ord(c)
                    printable_char_list.append(c)
                query_part = ''.join(printable_char_list)
                logging.debug('querying %s...', query_part)

            conn.query(query)
            r = conn.store_result()
            if r is not None:
                new_r = []
                for row in r.fetch_row(r.num_rows()):
                    new_row = []
                    for d in row:
                        if isinstance(d, array):
                            d = d.tostring()
                        new_row.append(d)
                    new_r.append(tuple(new_row))
                r = tuple(new_r)

        except OperationalError, m:
            if m[0] in (SERVER_GONE_ERROR, SERVER_LOST):
                logging.info('the MySQL server is gone; reconnecting')
                self.connect()
                return self.query(query)
            raise DatabaseFailure('MySQL error %d: %s' % (m[0], m[1]))
        return r

    def escape(self, s):
        """Escape special characters in a string."""
        return self.conn.escape_string(s)

    def setup(self, reset = 0):
        q = self.query

        if reset:
            q("""DROP TABLE IF EXISTS config, pt, trans, obj, ttrans, tobj""")

        # The table "config" stores configuration parameters which affect the
        # persistent data.
        q("""CREATE TABLE IF NOT EXISTS config (
                 name VARBINARY(16) NOT NULL PRIMARY KEY,
                 value VARBINARY(255) NOT NULL
             ) ENGINE = InnoDB""")

        # The table "pt" stores a partition table.
        q("""CREATE TABLE IF NOT EXISTS pt (
                 rid INT UNSIGNED NOT NULL,
                 uuid CHAR(32) NOT NULL,
                 state TINYINT UNSIGNED NOT NULL,
                 PRIMARY KEY (rid, uuid)
             ) ENGINE = InnoDB""")

        # The table "trans" stores information on committed transactions.
        q("""CREATE TABLE IF NOT EXISTS trans (
                 tid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL
             ) ENGINE = InnoDB""")

        # The table "obj" stores committed object data.
        q("""CREATE TABLE IF NOT EXISTS obj (
                 oid BIGINT UNSIGNED NOT NULL,
                 serial BIGINT UNSIGNED NOT NULL,
                 compression TINYINT UNSIGNED NOT NULL,
                 checksum INT UNSIGNED NOT NULL,
                 value MEDIUMBLOB NOT NULL,
                 PRIMARY KEY (oid, serial)
             ) ENGINE = InnoDB""")

        # The table "ttrans" stores information on uncommitted transactions.
        q("""CREATE TABLE IF NOT EXISTS ttrans (
                 tid BIGINT UNSIGNED NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL
             ) ENGINE = InnoDB""")

        # The table "tobj" stores uncommitted object data.
        q("""CREATE TABLE IF NOT EXISTS tobj (
                 oid BIGINT UNSIGNED NOT NULL,
                 serial BIGINT UNSIGNED NOT NULL,
                 compression TINYINT UNSIGNED NOT NULL,
                 checksum INT UNSIGNED NOT NULL,
                 value MEDIUMBLOB NOT NULL
             ) ENGINE = InnoDB""")

    def getConfiguration(self, key):
        q = self.query
        e = self.escape
        key = e(str(key))
        r = q("""SELECT value FROM config WHERE name = '%s'""" % key)
        try:
            return r[0][0]
        except IndexError:
            return None

    def _setConfiguration(self, key, value):
        q = self.query
        e = self.escape
        key = e(str(key))
        value = e(str(value))
        q("""REPLACE INTO config VALUES ('%s', '%s')""" % (key, value))

    def getPartitionTable(self):
        q = self.query
        cell_list = q("""SELECT rid, uuid, state FROM pt""")
        pt = []
        for offset, uuid, state in cell_list:
            uuid = util.bin(uuid)
            pt.append((offset, uuid, state))
        return pt

    def getLastTID(self, all = True):
        # XXX this does not consider serials in obj.
        # I am not sure if this is really harmful. For safety,
        # check for tobj only at the moment. The reason why obj is
        # not tested is that it is too slow to get the max serial
        # from obj when it has a huge number of objects, because
        # serial is the second part of the primary key, so the index
        # is not used in this case. If doing it, it is better to
        # make another index for serial, but I doubt the cost increase
        # is worth.
        q = self.query
        self.begin()
        ltid = q("""SELECT MAX(tid) FROM trans""")[0][0]
        if all:
            tmp_ltid = q("""SELECT MAX(tid) FROM ttrans""")[0][0]
            if ltid is None or (tmp_ltid is not None and ltid < tmp_ltid):
                ltid = tmp_ltid
            tmp_serial = q("""SELECT MAX(serial) FROM tobj""")[0][0]
            if ltid is None or (tmp_serial is not None and ltid < tmp_serial):
                ltid = tmp_serial
        self.commit()
        if ltid is not None:
            ltid = util.p64(ltid)
        return ltid

    def getUnfinishedTIDList(self):
        q = self.query
        tid_set = set()
        self.begin()
        r = q("""SELECT tid FROM ttrans""")
        tid_set.update((util.p64(t[0]) for t in r))
        r = q("""SELECT serial FROM tobj""")
        self.commit()
        tid_set.update((util.p64(t[0]) for t in r))
        return list(tid_set)

    def objectPresent(self, oid, tid, all = True):
        q = self.query
        oid = util.u64(oid)
        tid = util.u64(tid)
        self.begin()
        r = q("""SELECT oid FROM obj WHERE oid = %d AND serial = %d""" \
                % (oid, tid))
        if not r and all:
            r = q("""SELECT oid FROM tobj WHERE oid = %d AND serial = %d""" \
                    % (oid, tid))
        self.commit()
        if r:
            return True
        return False

    def getObject(self, oid, tid = None, before_tid = None):
        q = self.query
        oid = util.u64(oid)
        if tid is not None:
            tid = util.u64(tid)
            r = q("""SELECT serial, compression, checksum, value FROM obj
                        WHERE oid = %d AND serial = %d""" \
                    % (oid, tid))
            try:
                serial, compression, checksum, data = r[0]
                next_serial = None
            except IndexError:
                return None
        elif before_tid is not None:
            before_tid = util.u64(before_tid)
            r = q("""SELECT serial, compression, checksum, value FROM obj
                        WHERE oid = %d AND serial < %d
                        ORDER BY serial DESC LIMIT 1""" \
                    % (oid, before_tid))
            try:
                serial, compression, checksum, data = r[0]
            except IndexError:
                return None
            r = q("""SELECT serial FROM obj
                        WHERE oid = %d AND serial >= %d
                        ORDER BY serial LIMIT 1""" \
                    % (oid, before_tid))
            try:
                next_serial = r[0][0]
            except IndexError:
                next_serial = None
        else:
            # XXX I want to express "HAVING serial = MAX(serial)", but
            # MySQL does not use an index for a HAVING clause!
            r = q("""SELECT serial, compression, checksum, value FROM obj
                        WHERE oid = %d ORDER BY serial DESC LIMIT 1""" \
                    % oid)
            try:
                serial, compression, checksum, data = r[0]
                next_serial = None
            except IndexError:
                return None

        if serial is not None:
            serial = util.p64(serial)
        if next_serial is not None:
            next_serial = util.p64(next_serial)
        return serial, next_serial, compression, checksum, data

    def doSetPartitionTable(self, ptid, cell_list, reset):
        q = self.query
        e = self.escape
        self.begin()
        try:
            if reset:
                q("""TRUNCATE pt""")
            for offset, uuid, state in cell_list:
                uuid = e(util.dump(uuid))
                if state == CellStates.DISCARDED:
                    q("""DELETE FROM pt WHERE rid = %d AND uuid = '%s'""" \
                            % (offset, uuid))
                else:
                    q("""INSERT INTO pt VALUES (%d, '%s', %d)
                            ON DUPLICATE KEY UPDATE state = %d""" \
                                    % (offset, uuid, state, state))
            self.setPTID(ptid)
        except:
            self.rollback()
            raise
        self.commit()

    def changePartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, False)

    def setPartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, True)

    def dropPartition(self, num_partitions, offset):
        q = self.query
        self.begin()
        try:
            q("""DELETE FROM obj WHERE MOD(oid, %d) = %d""" %
                (num_partitions, offset))
            q("""DELETE FROM trans WHERE MOD(tid, %d) = %d""" %
                (num_partitions, offset))
        except:
            self.rollback()
            raise
        self.commit()

    def dropUnfinishedData(self):
        q = self.query
        self.begin()
        try:
            q("""TRUNCATE tobj""")
            q("""TRUNCATE ttrans""")
        except:
            self.rollback()
            raise
        self.commit()

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        q = self.query
        e = self.escape
        tid = util.u64(tid)

        if temporary:
            obj_table = 'tobj'
            trans_table = 'ttrans'
        else:
            obj_table = 'obj'
            trans_table = 'trans'

        self.begin()
        try:
            for oid, compression, checksum, data in object_list:
                oid = util.u64(oid)
                data = e(data)
                q("""REPLACE INTO %s VALUES (%d, %d, %d, %d, '%s')""" \
                        % (obj_table, oid, tid, compression, checksum, data))
            if transaction is not None:
                oid_list, user, desc, ext, packed = transaction
                packed = packed and 1 or 0
                oids = e(''.join(oid_list))
                user = e(user)
                desc = e(desc)
                ext = e(ext)
                q("""REPLACE INTO %s VALUES (%d, %i, '%s', '%s', '%s', '%s')""" \
                        % (trans_table, tid, packed, oids, user, desc, ext))
        except:
            self.rollback()
            raise
        self.commit()

    def finishTransaction(self, tid):
        q = self.query
        tid = util.u64(tid)
        self.begin()
        try:
            q("""INSERT INTO obj SELECT * FROM tobj WHERE tobj.serial = %d""" \
                    % tid)
            q("""DELETE FROM tobj WHERE serial = %d""" % tid)
            q("""INSERT INTO trans SELECT * FROM ttrans WHERE ttrans.tid = %d"""
                    % tid)
            q("""DELETE FROM ttrans WHERE tid = %d""" % tid)
        except:
            self.rollback()
            raise
        self.commit()

    def deleteTransaction(self, tid, all = False):
        q = self.query
        tid = util.u64(tid)
        self.begin()
        try:
            q("""DELETE FROM tobj WHERE serial = %d""" % tid)
            q("""DELETE FROM ttrans WHERE tid = %d""" % tid)
            if all:
                # Note that this can be very slow.
                q("""DELETE FROM obj WHERE serial = %d""" % tid)
                q("""DELETE FROM trans WHERE tid = %d""" % tid)
        except:
            self.rollback()
            raise
        self.commit()

    def getTransaction(self, tid, all = False):
        q = self.query
        tid = util.u64(tid)
        self.begin()
        r = q("""SELECT oids, user, description, ext, packed FROM trans
                    WHERE tid = %d""" \
                % tid)
        if not r and all:
            r = q("""SELECT oids, user, description, ext, packed FROM ttrans
                        WHERE tid = %d""" \
                    % tid)
        self.commit()
        if r:
            oids, user, desc, ext, packed = r[0]
            if (len(oids) % 8) != 0 or len(oids) == 0:
                raise DatabaseFailure('invalid oids for tid %x' % tid)
            oid_list = []
            for i in xrange(0, len(oids), 8):
                oid_list.append(oids[i:i+8])
            return oid_list, user, desc, ext, bool(packed)
        return None

    def getOIDList(self, offset, length, num_partitions, partition_list):
        q = self.query
        r = q("""SELECT DISTINCT oid FROM obj WHERE MOD(oid, %d) in (%s)
                    ORDER BY oid DESC LIMIT %d,%d""" \
                % (num_partitions, ','.join([str(p) for p in partition_list]),
                   offset, length))
        return [util.p64(t[0]) for t in r]

    def getObjectHistory(self, oid, offset = 0, length = 1):
        q = self.query
        oid = util.u64(oid)
        r = q("""SELECT serial, LENGTH(value) FROM obj WHERE oid = %d
                    ORDER BY serial DESC LIMIT %d, %d""" \
                % (oid, offset, length))
        if r:
            return [(util.p64(serial), length) for serial, length in r]
        return None

    def getTIDList(self, offset, length, num_partitions, partition_list):
        q = self.query
        r = q("""SELECT tid FROM trans WHERE MOD(tid, %d) in (%s)
                    ORDER BY tid DESC LIMIT %d,%d""" \
                % (num_partitions,
                   ','.join([str(p) for p in partition_list]),
                   offset, length))
        return [util.p64(t[0]) for t in r]

    def getReplicationTIDList(self, offset, length, num_partitions, partition_list):
        q = self.query
        r = q("""SELECT tid FROM trans WHERE MOD(tid, %d) in (%s)
                    ORDER BY tid ASC LIMIT %d,%d""" \
                % (num_partitions,
                   ','.join([str(p) for p in partition_list]),
                   offset, length))
        return [util.p64(t[0]) for t in r]

    def getTIDListPresent(self, tid_list):
        q = self.query
        r = q("""SELECT tid FROM trans WHERE tid in (%s)""" \
                % ','.join([str(util.u64(tid)) for tid in tid_list]))
        return [util.p64(t[0]) for t in r]

    def getSerialListPresent(self, oid, serial_list):
        q = self.query
        oid = util.u64(oid)
        r = q("""SELECT serial FROM obj WHERE oid = %d AND serial in (%s)""" \
                % (oid, ','.join([str(util.u64(serial)) for serial in serial_list])))
        return [util.p64(t[0]) for t in r]

