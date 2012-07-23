#
# Copyright (C) 2012  Nexedi SA
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

import sqlite3
from array import array
from hashlib import sha1
import re
import string
import traceback

from . import DatabaseManager, LOG_QUERIES
from .manager import CreationUndone
from neo.lib import logging, util
from neo.lib.exception import DatabaseFailure
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID, ZERO_HASH

def splitOIDField(tid, oids):
    if (len(oids) % 8) != 0 or len(oids) == 0:
        raise DatabaseFailure('invalid oids length for tid %d: %d' % (tid,
            len(oids)))
    oid_list = []
    append = oid_list.append
    for i in xrange(0, len(oids), 8):
        append(oids[i:i+8])
    return oid_list

def retry_if_locked(f, *args):
    try:
        return f(*args)
    except sqlite3.OperationalError, e:
        x = e.args[0]
        if x == 'database is locked':
            msg = traceback.format_exception_only(type(e), e)
            msg += traceback.format_stack()
            logging.warning(''.join(msg))
            while e.args[0] == x:
                try:
                    return f(*args)
                except sqlite3.OperationalError, e:
                    pass
        raise


class SQLiteDatabaseManager(DatabaseManager):
    """This class manages a database on SQLite.

    CAUTION: Make sure we never use statement journal files, as explained at
             http://www.sqlite.org/tempfiles.html for more information.
             In other words, temporary files (by default in /var/tmp !) must
             never be used for small requests.
    """

    def __init__(self, *args, **kw):
        super(SQLiteDatabaseManager, self).__init__(*args, **kw)
        self._config = {}
        self._connect()

    def _parse(self, database):
        self.db = database

    def close(self):
        self.conn.close()

    def _connect(self):
        logging.info('connecting to SQLite database %r', self.db)
        self.conn = sqlite3.connect(self.db, isolation_level=None,
            check_same_thread=False)

    def begin(self):
        q = self.query
        retry_if_locked(q, "BEGIN IMMEDIATE")
        return q

    if LOG_QUERIES:
        def commit(self):
            logging.debug('committing...')
            retry_if_locked(self.conn.commit)

        def rollback(self):
            logging.debug('aborting...')
            self.conn.rollback()

        def query(self, query):
            printable_char_list = []
            for c in query.split('\n', 1)[0][:70]:
                if c not in string.printable or c in '\t\x0b\x0c\r':
                    c = '\\x%02x' % ord(c)
                printable_char_list.append(c)
            logging.debug('querying %s...', ''.join(printable_char_list))
            return self.conn.execute(query)
    else:
        rollback = property(lambda self: self.conn.rollback)
        query = property(lambda self: self.conn.execute)
        def commit(self):
            retry_if_locked(self.conn.commit)

    def setup(self, reset = 0):
        self._config.clear()
        q = self.query

        if reset:
            for t in 'config', 'pt', 'trans', 'obj', 'data', 'ttrans', 'tobj':
                q('DROP TABLE IF EXISTS ' + t)

        # The table "config" stores configuration parameters which affect the
        # persistent data.
        q("""CREATE TABLE IF NOT EXISTS config (
                 name TEXT NOT NULL PRIMARY KEY,
                 value TEXT)
          """)

        # The table "pt" stores a partition table.
        q("""CREATE TABLE IF NOT EXISTS pt (
                 rid INTEGER NOT NULL,
                 uuid INTEGER NOT NULL,
                 state INTEGER NOT NULL,
                 PRIMARY KEY (rid, uuid))
          """)

        # The table "trans" stores information on committed transactions.
        q("""CREATE TABLE IF NOT EXISTS trans (
                 partition INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL,
                 PRIMARY KEY (partition, tid))
          """)

        # The table "obj" stores committed object metadata.
        q("""CREATE TABLE IF NOT EXISTS obj (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (partition, tid, oid))
          """)
        q("""CREATE INDEX IF NOT EXISTS _obj_i1 ON
                 obj(partition, oid, tid)
          """)
        q("""CREATE INDEX IF NOT EXISTS _obj_i2 ON
                 obj(data_id)
          """)

        # The table "data" stores object data.
        q("""CREATE TABLE IF NOT EXISTS data (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 hash BLOB NOT NULL UNIQUE,
                 compression INTEGER,
                 value BLOB)
          """)

        # The table "ttrans" stores information on uncommitted transactions.
        q("""CREATE TABLE IF NOT EXISTS ttrans (
                 partition INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL)
          """)

        # The table "tobj" stores uncommitted object metadata.
        q("""CREATE TABLE IF NOT EXISTS tobj (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (tid, oid))
          """)

        self._uncommitted_data = dict(q("SELECT data_id, count(*)"
            " FROM tobj WHERE data_id IS NOT NULL GROUP BY data_id"))

    def getConfiguration(self, key):
        try:
            return self._config[key]
        except KeyError:
            try:
                r = self.query("SELECT value FROM config WHERE name=?",
                               (key,)).fetchone()[0]
            except TypeError:
                r = None
            self._config[key] = r
            return r

    def _setConfiguration(self, key, value):
        q = self.query
        self._config[key] = value
        if value is None:
            q("DELETE FROM config WHERE name=?", (key,))
        else:
            q("REPLACE INTO config VALUES (?,?)", (key, str(value)))

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            return int(self.getConfiguration('_pack_tid'))
        except TypeError:
            return -1

    def getPartitionTable(self):
        return self.query("SELECT * FROM pt")

    def _getLastTIDs(self, all=True):
        p64 = util.p64
        with self as q:
            trans = dict((partition, p64(tid))
                for partition, tid in q("SELECT partition, MAX(tid)"
                                        " FROM trans GROUP BY partition"))
            obj = dict((partition, p64(tid))
                for partition, tid in q("SELECT partition, MAX(tid)"
                                        " FROM obj GROUP BY partition"))
            if all:
                tid = q("SELECT MAX(tid) FROM ttrans").fetchone()[0]
                if tid is not None:
                    trans[None] = p64(tid)
                tid = q("SELECT MAX(tid) FROM tobj").fetchone()[0]
                if tid is not None:
                    obj[None] = p64(tid)
        return trans, obj

    def getUnfinishedTIDList(self):
        p64 = util.p64
        tid_set = set()
        with self as q:
            tid_set.update((p64(t[0]) for t in q("SELECT tid FROM ttrans")))
            tid_set.update((p64(t[0]) for t in q("SELECT tid FROM tobj")))
        return list(tid_set)

    def objectPresent(self, oid, tid, all=True):
        oid = util.u64(oid)
        tid = util.u64(tid)
        with self as q:
            r = q("SELECT 1 FROM obj WHERE partition=? AND oid=? AND tid=?",
                  (self._getPartition(oid), oid, tid)).fetchone()
            if not r and all:
                r = q("SELECT 1 FROM tobj WHERE tid=? AND oid=?",
                      (tid, oid)).fetchone()
        return bool(r)

    def _getObject(self, oid, tid=None, before_tid=None):
        q = self.query
        partition = self._getPartition(oid)
        sql = ('SELECT tid, compression, data.hash, value, value_tid'
               ' FROM obj LEFT JOIN data ON obj.data_id = data.id'
               ' WHERE partition=? AND oid=?')
        if tid is not None:
            r = q(sql + ' AND tid=?', (partition, oid, tid))
        elif before_tid is not None:
            r = q(sql + ' AND tid<? ORDER BY tid DESC LIMIT 1',
                  (partition, oid, before_tid))
        else:
            r = q(sql + ' ORDER BY tid DESC LIMIT 1', (partition, oid))
        try:
            serial, compression, checksum, data, value_serial = r.fetchone()
        except TypeError:
            return None
        r = q("""SELECT tid FROM obj
                 WHERE partition=? AND oid=? AND tid>?
                 ORDER BY tid LIMIT 1""",
              (partition, oid, serial)).fetchone()
        if checksum:
            checksum = str(checksum)
            data = str(data)
        return serial, r and r[0], compression, checksum, data, value_serial

    def doSetPartitionTable(self, ptid, cell_list, reset):
        with self as q:
            if reset:
                q("DELETE FROM pt")
            for offset, uuid, state in cell_list:
                # TODO: this logic should move out of database manager
                # add 'dropCells(cell_list)' to API and use one query
                # WKRD: Why does SQLite need a statement journal file
                #       whereas we try to replace only 1 value ?
                #       We don't want to remove the 'NOT NULL' constraint
                #       so we must simulate a "REPLACE OR FAIL".
                q("DELETE FROM pt WHERE rid=? AND uuid=?", (offset, uuid))
                if state != CellStates.DISCARDED:
                    q("INSERT OR FAIL INTO pt VALUES (?,?,?)",
                      (offset, uuid, int(state)))
            self.setPTID(ptid)

    def changePartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, False)

    def setPartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, True)

    def dropPartitions(self, offset_list):
        where = " WHERE partition=?"
        with self as q:
            for partition in offset_list:
                data_id_list = [x for x, in
                    q("SELECT DISTINCT data_id FROM obj" + where,
                        (partition,)) if x]
                q("DELETE FROM obj" + where, (partition,))
                q("DELETE FROM trans" + where, (partition,))
                self._pruneData(data_id_list)

    def dropUnfinishedData(self):
        with self as q:
            data_id_list = [x for x, in q("SELECT data_id FROM tobj") if x]
            q("DELETE FROM tobj")
            q("DELETE FROM ttrans")
        self.unlockData(data_id_list, True)

    def storeTransaction(self, tid, object_list, transaction, temporary=True):
        u64 = util.u64
        tid = u64(tid)
        T = 't' if temporary else ''
        obj_sql = "INSERT OR FAIL INTO %sobj VALUES (?,?,?,?,?)" % T
        with self as q:
            for oid, data_id, value_serial in object_list:
                oid = u64(oid)
                partition = self._getPartition(oid)
                if value_serial:
                    value_serial = u64(value_serial)
                    (data_id,), = q("SELECT data_id FROM obj"
                        " WHERE partition=? AND oid=? AND tid=?",
                        (partition, oid, value_serial))
                    if temporary:
                        self.storeData(data_id)
                try:
                    q(obj_sql, (partition, oid, tid, data_id, value_serial))
                except sqlite3.IntegrityError:
                    # This may happen if a previous replication of 'obj' was
                    # interrupted.
                    if not T:
                        r, = q("SELECT data_id, value_tid FROM obj"
                               " WHERE partition=? AND oid=? AND tid=?",
                               (partition, oid, tid))
                        if r == (data_id, value_serial):
                            continue
                    raise

            if transaction:
                oid_list, user, desc, ext, packed, ttid = transaction
                partition = self._getPartition(tid)
                assert packed in (0, 1)
                q("INSERT OR FAIL INTO %strans VALUES (?,?,?,?,?,?,?,?)" % T,
                    (partition, tid, packed, buffer(''.join(oid_list)),
                     buffer(user), buffer(desc), buffer(ext), u64(ttid)))

    def _pruneData(self, data_id_list):
        data_id_list = set(data_id_list).difference(self._uncommitted_data)
        if data_id_list:
            q = self.query
            data_id_list.difference_update(x for x, in q(
                "SELECT DISTINCT data_id FROM obj WHERE data_id IN (%s)"
                % ",".join(map(str, data_id_list))))
            q("DELETE FROM data WHERE id IN (%s)"
              % ",".join(map(str, data_id_list)))

    def _storeData(self, checksum, data, compression):
        H = buffer(checksum)
        with self as q:
            try:
                return q("INSERT INTO data VALUES (NULL,?,?,?)",
                    (H, compression,  buffer(data))).lastrowid
            except sqlite3.IntegrityError, e:
                if e.args[0] == 'column hash is not unique':
                    (r, c, d), = q("SELECT id, compression, value"
                                  " FROM data WHERE hash=?",  (H,))
                    if c == compression and str(d) == data:
                        return r
                raise

    def _getDataTID(self, oid, tid=None, before_tid=None):
        partition = self._getPartition(oid)
        sql = 'SELECT tid, data_id, value_tid FROM obj' \
              ' WHERE partition=? AND oid=?'
        if tid is not None:
            r = self.query(sql + ' AND tid=?', (partition, oid, tid))
        elif before_tid is not None:
            r = self.query(sql + ' AND tid<? ORDER BY tid DESC LIMIT 1',
                           (partition, oid, before_tid))
        else:
            r = self.query(sql + ' ORDER BY tid DESC LIMIT 1',
                           (partition, oid))
        r = r.fetchone()
        if r:
            serial, data_id, value_serial = r
            if value_serial is None and data_id:
                return serial, serial
            return serial, value_serial
        return None, None

    def finishTransaction(self, tid):
        args = util.u64(tid),
        with self as q:
            sql = " FROM tobj WHERE tid=?"
            data_id_list = [x for x, in q("SELECT data_id" + sql, args) if x]
            q("INSERT OR FAIL INTO obj SELECT *" + sql, args)
            q("DELETE FROM tobj WHERE tid=?", args)
            q("INSERT OR FAIL INTO trans SELECT * FROM ttrans WHERE tid=?",
              args)
            q("DELETE FROM ttrans WHERE tid=?", args)
        self.unlockData(data_id_list)

    def deleteTransaction(self, tid, oid_list=()):
        u64 = util.u64
        tid = u64(tid)
        getPartition = self._getPartition
        with self as q:
            sql = " FROM tobj WHERE tid=?"
            data_id_list = [x for x, in q("SELECT data_id" + sql, (tid,)) if x]
            self.unlockData(data_id_list)
            q("DELETE" + sql, (tid,))
            q("DELETE FROM ttrans WHERE tid=?", (tid,))
            q("DELETE FROM trans WHERE partition=? AND tid=?",
                (getPartition(tid), tid))
            # delete from obj using indexes
            data_id_set = set()
            for oid in oid_list:
                oid = u64(oid)
                sql = " FROM obj WHERE partition=? AND oid=? AND tid=?"
                args = getPartition(oid), oid, tid
                data_id_set.update(*q("SELECT data_id" + sql, args))
                q("DELETE" + sql, args)
            data_id_set.discard(None)
            self._pruneData(data_id_set)

    def deleteObject(self, oid, serial=None):
        oid = util.u64(oid)
        sql = " FROM obj WHERE partition=? AND oid=?"
        args = [self._getPartition(oid), oid]
        if serial:
            sql += " AND tid=?"
            args.append(util.u64(serial))
        with self as q:
            data_id_list = [x for x, in q("SELECT DISTINCT data_id" + sql, args)
                              if x]
            q("DELETE" + sql, args)
            self._pruneData(data_id_list)

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        sql = " WHERE partition=?"
        args = [partition]
        if min_tid:
            sql += " AND ? < tid"
            args.append(util.u64(min_tid))
        if max_tid:
            sql += " AND tid <= ?"
            args.append(util.u64(max_tid))
        q = self.query
        q("DELETE FROM trans" + sql, args)
        sql = " FROM obj" + sql
        data_id_list = [x for x, in q("SELECT DISTINCT data_id" + sql, args)
                          if x]
        q("DELETE" + sql, args)
        self._pruneData(data_id_list)

    def getTransaction(self, tid, all=False):
        tid = util.u64(tid)
        with self as q:
            r = q("SELECT oids, user, description, ext, packed, ttid"
                  " FROM trans WHERE partition=? AND tid=?",
                  (self._getPartition(tid), tid)).fetchone()
            if not r and all:
                r = q("SELECT oids, user, description, ext, packed, ttid"
                      " FROM ttrans WHERE tid=?", (tid,)).fetchone()
        if r:
            oids, user, description, ext, packed, ttid = r
            return splitOIDField(tid, oids), str(user), \
                str(description), str(ext), packed, util.p64(ttid)

    def _getObjectLength(self, oid, value_serial):
        if value_serial is None:
            raise CreationUndone
        length, value_serial = self.query("""SELECT LENGTH(value), value_tid
            FROM obj LEFT JOIN data ON obj.data_id=data.id
            WHERE partition=? AND oid=? AND tid=?""",
            (self._getPartition(oid), oid, value_serial)).fetchone()
        if length is None:
            logging.info("Multiple levels of indirection"
                " when searching for object data for oid %d at tid %d."
                " This causes suboptimal performance.", oid, value_serial)
            length = self._getObjectLength(oid, value_serial)
        return length

    def getObjectHistory(self, oid, offset=0, length=1):
        # FIXME: This method doesn't take client's current transaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        p64 = util.p64
        oid = util.u64(oid)
        pack_tid = self._getPackTID()
        result = []
        append = result.append
        with self as q:
            for serial, length, value_serial in q("""\
                    SELECT tid, LENGTH(value), value_tid
                    FROM obj LEFT JOIN data ON obj.data_id = data.id
                    WHERE partition=? AND oid=? AND tid>=?
                    ORDER BY tid DESC LIMIT ?,?""",
                    (self._getPartition(oid), oid, pack_tid, offset, length)):
                if length is None:
                    try:
                        length = self._getObjectLength(oid, value_serial)
                    except CreationUndone:
                        length = 0
                append((p64(serial), length))
        return result or None

    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        return [(p64(serial), p64(oid)) for serial, oid in self.query("""\
            SELECT tid, oid FROM obj
            WHERE partition=? AND tid<=?
            AND (tid=? AND ?<=oid OR ?<tid)
            ORDER BY tid ASC, oid ASC LIMIT ?""",
            (partition, u64(max_tid), min_tid, u64(min_oid), min_tid, length))]

    def getTIDList(self, offset, length, partition_list):
        p64 = util.p64
        return [p64(t[0]) for t in self.query("""\
            SELECT tid FROM trans WHERE partition in (%s)
            ORDER BY tid DESC LIMIT %d,%d"""
            % (','.join(map(str, partition_list)), offset, length))]

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        max_tid = u64(max_tid)
        return [p64(t[0]) for t in self.query("""\
            SELECT tid FROM trans
            WHERE partition=? AND ?<=tid AND tid<=?
            ORDER BY tid ASC LIMIT ?""",
            (partition, min_tid, max_tid, length))]

    def _updatePackFuture(self, oid, orig_serial, max_serial):
        # Before deleting this objects revision, see if there is any
        # transaction referencing its value at max_serial or above.
        # If there is, copy value to the first future transaction. Any further
        # reference is just updated to point to the new data location.
        partition = self._getPartition(oid)
        value_serial = None
        q = self.query
        for T in '', 't':
            update = """UPDATE OR FAIL %sobj SET value_tid=?
                         WHERE partition=? AND oid=? AND tid=?""" % T
            for serial, in q("""SELECT tid FROM %sobj
                    WHERE partition=? AND oid=? AND tid>=? AND value_tid=?
                    ORDER BY tid ASC""" % T,
                    (partition, oid, max_serial, orig_serial)):
                q(update, (value_serial, partition, oid, serial))
                if value_serial is None:
                    # First found, mark its serial for future reference.
                    value_serial = serial
        return value_serial

    def pack(self, tid, updateObjectDataForPack):
        # TODO: unit test (along with updatePackFuture)
        p64 = util.p64
        tid = util.u64(tid)
        updatePackFuture = self._updatePackFuture
        getPartition = self._getPartition
        with self as q:
            self._setPackTID(tid)
            for count, oid, max_serial in q("SELECT COUNT(*) - 1, oid,"
                    " MAX(tid) FROM obj WHERE tid<=? GROUP BY oid",
                    (tid,)):
                partition = getPartition(oid)
                if q("SELECT 1 FROM obj WHERE partition=?"
                     " AND oid=? AND tid=? AND data_id IS NULL",
                     (partition, oid, max_serial)).fetchone():
                    max_serial += 1
                elif not count:
                    continue
                # There are things to delete for this object
                data_id_set = set()
                sql = " FROM obj WHERE partition=? AND oid=? AND tid<?"
                args = partition, oid, max_serial
                for serial, data_id in q("SELECT tid, data_id" + sql, args):
                    data_id_set.add(data_id)
                    new_serial = updatePackFuture(oid, serial, max_serial)
                    if new_serial:
                        new_serial = p64(new_serial)
                    updateObjectDataForPack(p64(oid), p64(serial),
                                            new_serial, data_id)
                q("DELETE" + sql, args)
                data_id_set.discard(None)
                self._pruneData(data_id_set)

    def checkTIDRange(self, partition, length, min_tid, max_tid):
        count, tids, max_tid = self.query("""\
            SELECT COUNT(*), GROUP_CONCAT(tid), MAX(tid)
            FROM (SELECT tid FROM trans
                  WHERE partition=? AND ?<=tid AND tid<=?
                  ORDER BY tid ASC LIMIT ?) AS t""",
            (partition, util.u64(min_tid), util.u64(max_tid),
             -1 if length is None else length)).fetchone()
        if count:
            return count, sha1(tids).digest(), util.p64(max_tid)
        return 0, ZERO_HASH, ZERO_TID

    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        u64 = util.u64
        # We don't ask MySQL to compute everything (like in checkTIDRange)
        # because it's difficult to get the last serial _for the last oid_.
        # We would need a function (that could be named 'LAST') that returns the
        # last grouped value, instead of the greatest one.
        min_tid = u64(min_tid)
        r = self.query("""\
            SELECT tid, oid
            FROM obj
            WHERE partition=? AND tid<=? AND (tid>? OR tid=? AND oid>=?)
            ORDER BY tid, oid LIMIT ?""",
            (partition, u64(max_tid), min_tid, min_tid, u64(min_oid),
             -1 if length is None else length)).fetchall()
        if r:
            p64 = util.p64
            return (len(r),
                    sha1(','.join(str(x[0]) for x in r)).digest(),
                    p64(r[-1][0]),
                    sha1(','.join(str(x[1]) for x in r)).digest(),
                    p64(r[-1][1]))
        return 0, ZERO_HASH, ZERO_TID, ZERO_HASH, ZERO_OID
