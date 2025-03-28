#
# Copyright (C) 2012-2019  Nexedi SA
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

from collections import defaultdict, OrderedDict
import os
import sqlite3
from hashlib import sha1
import string
import subprocess
import traceback
from urlparse import urlsplit, parse_qsl

from . import LOG_QUERIES, DatabaseFailure
from .manager import DatabaseManager, splitOIDField
from neo.lib import logging, util
from neo.lib.exception import NonReadableCell, UndoPackError
from neo.lib.interfaces import implements
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID, ZERO_HASH, MAX_TID

def unique_constraint_message(table, *columns):
    c = sqlite3.connect(":memory:")
    values = '?' * len(columns)
    insert = "INSERT INTO %s VALUES(%s)" % (table, ', '.join(values))
    x = "%s (%s)" % (table, ', '.join(columns))
    c.execute("CREATE TABLE " + x)
    c.execute("CREATE UNIQUE INDEX i ON " + x)
    try:
        c.executemany(insert, (values, values))
    except sqlite3.IntegrityError as e:
        return e.args[0]
    assert False

def retry_if_locked(f, *args):
    try:
        return f(*args)
    except sqlite3.OperationalError as e:
        x = e.args[0]
        if x != 'database is locked':
            raise
        msg = traceback.format_exception_only(type(e), e)
        msg += traceback.format_stack()
        logging.warning(''.join(msg))
        while 1:
            try:
                return f(*args)
            except sqlite3.OperationalError as e:
                if e.args[0] != x:
                    raise


@implements
class SQLiteDatabaseManager(DatabaseManager):
    """This class manages a database on SQLite.

    CAUTION: Make sure we never use statement journal files, as explained at
             https://www.sqlite.org/tempfiles.html for more information.
             In other words, temporary files (by default in /var/tmp !) must
             never be used for small requests.
    """

    VERSION = 4

    cksumvfs = None

    def _parse(self, database):
        pragmas = self.pragmas = {}
        if database.startswith('file:'):
            database = urlsplit(database)
            for k, v in parse_qsl(database.query, keep_blank_values=True,
                                  strict_parsing=True):
                if k == 'cksumvfs':
                    if self.cksumvfs is not None or v not in ('0', '1'):
                        raise DatabaseFailure(
                            "invalid or duplicate %s value" % k)
                    self.cksumvfs = int(v)
                    continue
                if k in pragmas:
                    raise DatabaseFailure("duplicate pragma: " + k)
                if k not in ('cache_size', 'journal_mode', 'synchronous'):
                    raise DatabaseFailure("unsupported pragma: " + k)
                pragmas[k] = v
            self.db = database.path
        else:
            self.db = os.path.expanduser(database)
        if self.cksumvfs:
            db = sqlite3.connect(':memory:')
            try:
                db.enable_load_extension(True)
                db.load_extension('cksumvfs.so')
            finally:
                db.close()
        if self.UNSAFE:
            pragmas.setdefault('synchronous', 'OFF')
            pragmas.setdefault('journal_mode', 'MEMORY')

    def _close(self):
        self.conn.close()

    def _connect(self):
        logging.info('connecting to SQLite database %r', self.db)
        if self.cksumvfs and not os.path.exists(self.db):
            subprocess.check_output(('sqlite3', ':memory:',
                '.load cksumvfs.so',
                '.open "%s"' % self.db.replace('\\', r'\\').replace('"', r'\"'),
                '.filectrl reserve_bytes 8',
                'vacuum')) # PY3: check_call(..., stdout=subprocess.DEVNULL)
            os.stat(self.db)
        self.conn = sqlite3.connect(self.db, check_same_thread=False)
        self.conn.text_factory = str
        self.lockFile(self.db)
        try:
            if self.cksumvfs:
                self.cksumvfs = 0
                (x,), = self.query("PRAGMA checksum_verification").fetchall()
                if not int(x):
                    raise DatabaseFailure("Can't enable cksumvfs.")
            for pragma in map("PRAGMA %s = %s".__mod__,
                              self.pragmas.iteritems()):
                logging.info(pragma)
                self.query(pragma).fetchall()
        except Exception:
            self.close()
            raise
        self._config = {}

    def _getDevPath(self):
        return self.db

    def _commit(self):
        retry_if_locked(self.conn.commit)

    if LOG_QUERIES:
        def query(self, query, *args):
            printable_char_list = []
            for c in query.split('\n', 1)[0][:70]:
                if c not in string.printable or c in '\t\x0b\x0c\r':
                    c = '\\x%02x' % ord(c)
                printable_char_list.append(c)
            logging.debug('querying %s...', ''.join(printable_char_list))
            return self.conn.execute(query, *args)
    else:
        query = property(lambda self: self.conn.execute)

    def erase(self):
        for t in ('config', 'pt', 'pack', 'trans',
                  'obj', 'data', 'ttrans', 'tobj'):
            self.query('DROP TABLE IF EXISTS ' + t)

    def nonempty(self, table):
        # WKRD: At least up to Python 3.11, there's a sqlite3 bug that
        #       may cause weird exceptions (sometimes even SystemError!)
        #       when querying directly a non-existing table,
        #       instead of something equivalent to MySQL's NO_SUCH_TABLE.
        #       Not sure those exceptions always mean the table does not exit
        #       so better first check sqlite_master.
        q = self.query
        if q("SELECT * FROM sqlite_master WHERE type='table' AND name=?",
             (table,)).fetchone():
            return bool(q("SELECT 1 FROM %s LIMIT 1" % table).fetchone())

    def _alterTable(self, schema_dict, table, select="*"):
        # BBB: As explained in _setup, no transactional DDL
        #      so let's do the same dance as for MySQL.
        q = self.query
        new = 'new_' + table
        if self.nonempty(table) is None:
            if self.nonempty(new) is None:
                return
        else:
            q("DROP TABLE IF EXISTS " + new)
            q(schema_dict.pop(table) % new)
            q("INSERT INTO %s SELECT %s FROM %s" % (new, select, table))
            q("DROP TABLE " + table)
        q("ALTER TABLE %s RENAME TO %s" % (new, table))

    def _migrate1(self, *_):
        self._checkNoUnfinishedTransactions()
        self.query("DROP TABLE IF EXISTS ttrans")

    def _migrate2(self, schema_dict, index_dict):
        self._alterTable(schema_dict, 'obj')

    def _migrate3(self, schema_dict, index_dict):
        x = 'pt'
        self._alterTable({x: schema_dict[x].replace('pack', '-- pack')}, x,
            "rid, nid, CASE state"
            " WHEN 0 THEN -1"  # UP_TO_DATE
            " WHEN 2 THEN -2"  # FEEDING
            " ELSE 1-state END")

    def _migrate4(self, schema_dict, index_dict):
        self._setConfiguration('partitions', None)
        self._alterTable(schema_dict, 'pt', "*, CASE"
            " WHEN nid=%s THEN 0"
            " ELSE NULL END" % self.getUUID())

    def _setup(self, dedup=False):
        # BBB: SQLite has transactional DDL but before Python 3.6,
        #      the binding automatically commits between such statements.
        #      This anti-feature causes this method to be relatively slow.
        #      Unit tests enables the UNSAFE boolean flag.
        self._config.clear()
        q = self.query
        schema_dict = OrderedDict()
        index_dict = {}

        # The table "config" stores configuration
        # parameters which affect the persistent data.
        schema_dict['config'] = """CREATE TABLE %s (
                 name TEXT NOT NULL PRIMARY KEY,
                 value TEXT) WITHOUT ROWID
            """

        # The table "pt" stores a partition table.
        schema_dict['pt'] = """CREATE TABLE %s (
                 partition INTEGER NOT NULL,
                 nid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 pack INTEGER,
                 PRIMARY KEY (partition, nid)
            ) WITHOUT ROWID
            """

        schema_dict['pack'] = """CREATE TABLE %s (
                 tid INTEGER PRIMARY KEY,
                 approved BOOLEAN, -- NULL if not signed
                 partial BOOLEAN NOT NULL,
                 oids BLOB, -- same format as trans.oids
                 pack_tid INTEGER)
            """

        # The table "trans" stores information on committed transactions.
        schema_dict['trans'] = """CREATE TABLE %s (
                 partition INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL,
                 PRIMARY KEY (partition, tid)
            ) WITHOUT ROWID
            """

        # The table "obj" stores committed object metadata.
        schema_dict['obj'] = """CREATE TABLE %s (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (partition, oid, tid)
            ) WITHOUT ROWID
            """
        index_dict['obj'] = (
            "CREATE INDEX %s ON %s(partition, tid, oid)",
            "CREATE INDEX %s ON %s(data_id)")

        # The table "data" stores object data.
        schema_dict['data'] = """CREATE TABLE %s (
                 id INTEGER PRIMARY KEY,
                 hash BLOB NOT NULL,
                 compression INTEGER NOT NULL,
                 value BLOB NOT NULL)
            """
        if dedup:
            index_dict['data'] = (
                "CREATE UNIQUE INDEX %s ON %s(hash, compression)",)

        # The table "ttrans" stores information on uncommitted transactions.
        schema_dict['ttrans'] = """CREATE TABLE %s (
                 partition INTEGER NOT NULL,
                 tid INTEGER,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL)
            """

        # The table "tobj" stores uncommitted object metadata.
        schema_dict['tobj'] = """CREATE TABLE %s (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (tid, oid)
            ) WITHOUT ROWID
            """

        if self.nonempty('config') is None:
            q(schema_dict.pop('config') % 'config')
            self._setConfiguration('version', self.VERSION)
        else:
            self.migrate(schema_dict, index_dict)

        for table, schema in schema_dict.iteritems():
            q(schema % ('IF NOT EXISTS ' + table))
        for table, index in index_dict.iteritems():
            for i, index in enumerate(index, 1):
                q(index % ('IF NOT EXISTS _%s_i%s' % (table, i), table))

        self._uncommitted_data.update(q("SELECT data_id, count(*)"
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

    def _getMaxPartition(self):
        return self.query("SELECT MAX(`partition`) FROM pt").next()[0]

    def _getPartitionTable(self):
        return self.query("SELECT * FROM pt")

    def _getFirstTID(self, partition):
        tid = self.query("SELECT MIN(tid) FROM trans WHERE partition=?",
                         (partition,)).fetchone()[0]
        return util.u64(MAX_TID) if tid is None else tid

    def _getLastTID(self, partition, max_tid=None):
        x = self.query
        if max_tid is None:
            x = x("SELECT MAX(tid) FROM trans WHERE partition=?", (partition,))
        else:
            x = x("SELECT MAX(tid) FROM trans WHERE partition=? AND tid<=?",
                  (partition, max_tid))
        return x.next()[0]

    def _getLastIDs(self, *args):
        q = self.query
        (oid,), = q("SELECT MAX(oid) FROM obj WHERE `partition`=?", args)
        (tid,), = q("SELECT MAX(tid) FROM obj WHERE `partition`=?", args)
        return tid, oid

    def _getPackOrders(self, min_completed):
        return self.query(
            "SELECT * FROM pack WHERE tid >= ? AND tid %% %s IN (%s)"
            " ORDER BY tid"
            % (self.np, ','.join(map(str, self._readable_set))),
            (min_completed,))

    def getPackedIDs(self, up_to_date=False):
        return {offset: util.p64(pack) for offset, pack in self.query(
            "SELECT partition, pack FROM pt WHERE pack IS NOT NULL"
            + (" AND tid=-%u" % CellStates.UP_TO_DATE if up_to_date else ""))}

    def _getPartitionPacked(self, partition):
        (pack_id,), = self.query(
            "SELECT pack FROM pt WHERE partition=? AND nid=?",
            (partition, self.getUUID()))
        assert pack_id is not None # PY3: the assertion will be useless because
                                   #      the caller always compares the value
        return pack_id

    def _setPartitionPacked(self, partition, pack_id):
        assert pack_id is not None
        self.query("UPDATE pt SET pack=? WHERE partition=? AND nid=?",
                   (pack_id, partition, self.getUUID()))

    def _getDataLastId(self, partition):
        return self.query("SELECT MAX(id) FROM data WHERE %s <= id AND id < %s"
            % (partition << 48, (partition + 1) << 48)).fetchone()[0]

    def _getUnfinishedTIDDict(self):
        q = self.query
        return q("SELECT ttid, tid FROM ttrans"), (ttid
            for ttid, in q("SELECT DISTINCT tid FROM tobj"))

    def getFinalTID(self, ttid):
        ttid = util.u64(ttid)
        # As of SQLite 3.8.7.1, 'tid>=ttid' would ignore the index on tid,
        # even though ttid is a constant.
        for tid, in self.query("SELECT tid FROM trans"
                " WHERE partition=? AND tid>=? AND ttid=? LIMIT 1",
                (self._getReadablePartition(ttid), ttid, ttid)):
            return util.p64(tid)

    def getLastObjectTID(self, oid):
        oid = util.u64(oid)
        r = self.query("SELECT tid FROM obj"
                       " WHERE partition=? AND oid=?"
                       " ORDER BY tid DESC LIMIT 1",
                       (self._getReadablePartition(oid), oid)).fetchone()
        return r and util.p64(r[0])

    def _getNextTID(self, *args): # partition, oid, tid
        r = self.query("""SELECT tid FROM obj
                          WHERE partition=? AND oid=? AND tid>?
                          ORDER BY tid LIMIT 1""", args).fetchone()
        return r and r[0]

    def _getObject(self, oid, tid=None, before_tid=None):
        q = self.query
        partition = self._getReadablePartition(oid)
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
        if checksum:
            checksum = str(checksum)
            data = str(data)
        return (serial, self._getNextTID(partition, oid, serial),
                compression, checksum, data, value_serial)

    def _changePartitionTable(self, cell_list, reset=False):
        q = self.query
        delete = set(q("SELECT partition, nid FROM pt")) if reset else set()
        for cell in cell_list:
            key = cell[:2]
            tid = cell[2]
            if tid is None:
                delete.add(key)
            else:
                delete.discard(key)
                if q("SELECT 1 FROM pt WHERE partition=? AND nid=?",
                       key).fetchone():
                    q("UPDATE pt SET tid=? WHERE partition=? AND nid=?",
                      (tid,) + key)
                else:
                    q("INSERT OR FAIL INTO pt VALUES (?,?,?,?)", cell)
        for key in delete:
            q("DELETE FROM pt WHERE partition=? AND nid=?", key)

    def _dropPartition(self, *args):
        q = self.query
        where = " FROM obj WHERE partition=? ORDER BY tid, oid LIMIT ?"
        x = q("SELECT DISTINCT data_id" + where, args).fetchall()
        if x:
            q("DELETE" + where, args)
            return [x for x, in x]
        x = args[:1]
        q("DELETE FROM pack WHERE tid IN ("
            "SELECT tid FROM trans JOIN pack USING (tid) WHERE partition=?)", x)
        return q("DELETE FROM trans WHERE partition=?", x).rowcount

    def _getUnfinishedDataIdList(self):
        return [x for x, in self.query(
            "SELECT data_id FROM tobj WHERE data_id IS NOT NULL")]

    def dropPartitionsTemporary(self, offset_list=None):
        where = "" if offset_list is None else \
            " WHERE `partition` IN (%s)" % ','.join(map(str, offset_list))
        q = self.query
        q("DELETE FROM tobj" + where)
        q("DELETE FROM pack WHERE tid IN ("
            "SELECT tid FROM ttrans JOIN pack USING (tid)%s)" % where)
        q("DELETE FROM ttrans" + where)

    def storeTransaction(self, tid, object_list, transaction, temporary=True):
        u64 = util.u64
        tid = u64(tid)
        T = 't' if temporary else ''
        obj_sql = "INSERT OR FAIL INTO %sobj VALUES (?,?,?,?,?)" % T
        q = self.query
        for oid, data_id, value_serial in object_list:
            oid = u64(oid)
            partition = self._getPartition(oid)
            if value_serial:
                value_serial = u64(value_serial)
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
                (partition, None if temporary else tid,
                 packed, buffer(''.join(oid_list)),
                 buffer(user), buffer(desc), buffer(ext), u64(ttid)))

    def getOrphanList(self):
        return [x for x, in self.query(
            "SELECT id FROM data LEFT JOIN obj ON (id=data_id)"
            " WHERE data_id IS NULL")]

    def _pruneData(self, data_id_list):
        data_id_list = set(data_id_list).difference(self._uncommitted_data)
        if data_id_list:
            q = self.query
            data_id_list.difference_update(x for x, in q(
                "SELECT DISTINCT data_id FROM obj WHERE data_id IN (%s)"
                % ",".join(map(str, data_id_list))))
            q("DELETE FROM data WHERE id IN (%s)"
              % ",".join(map(str, data_id_list)))
            return len(data_id_list)
        return 0

    def storeData(self, checksum, oid, data, compression, data_tid,
            _dup=unique_constraint_message("data", "hash", "compression")):
        oid = util.u64(oid)
        p = self._getPartition(oid)
        if data_tid:
            for r, in self.query("SELECT data_id FROM obj"
                    " WHERE partition=? AND oid=? AND tid=?",
                    (p, oid, util.u64(data_tid))):
                return r
            if p in self._readable_set: # and not checksum:
                raise UndoPackError
        if not checksum:
            return # delete
        H = buffer(checksum)
        r = self._data_last_ids[p]
        try:
            self.query("INSERT INTO data VALUES (?,?,?,?)",
                (r, H, compression,  buffer(data)))
        except sqlite3.IntegrityError as e:
            if e.args[0] == _dup:
                (r, d), = self.query("SELECT id, value FROM data"
                                     " WHERE hash=? AND compression=?",
                                     (H, compression))
                if str(d) == data:
                    return r
            raise
        self._data_last_ids[p] = r + 1
        return r

    def loadData(self, data_id):
        compression, checksum, data = self.query(
            "SELECT compression, hash, value  FROM data WHERE id=?",
            (data_id,)).fetchone()
        if checksum:
            return compression, str(checksum), str(data)
        return compression, checksum, data

    def storePackOrder(self, tid, approved, partial, oid_list, pack_tid):
        u64 = util.u64
        self.query("INSERT INTO pack VALUES (?,?,?,?,?)", (
            u64(tid), approved, partial,
            None if oid_list is None else buffer(''.join(oid_list)),
            u64(pack_tid)))

    def _signPackOrders(self, approved, rejected):
        def isTID(x):
            return "tid IN (%s)" % ','.join(map(str, x)) if x else 0
        approved = isTID(approved)
        where = " WHERE %s OR %s" % (approved, isTID(rejected))
        changed = [tid for tid, in self.query("SELECT tid FROM pack" + where)]
        if changed:
            self.query("UPDATE pack SET approved = %s%s" % (approved, where))
        return changed

    def lockTransaction(self, tid, ttid, pack):
        u64 = util.u64
        self.query("UPDATE ttrans SET tid=? WHERE ttid=?",
                   (u64(tid), u64(ttid)))
        if pack:
            self.query("UPDATE pack SET approved=1 WHERE tid=?", (u64(ttid),))
        self.commit()

    def unlockTransaction(self, tid, ttid, trans, obj, pack):
        q = self.query
        u64 = util.u64
        tid = u64(tid)
        if trans:
            q("INSERT INTO trans SELECT * FROM ttrans WHERE tid=?", (tid,))
            if pack:
                q("UPDATE pack SET tid=? WHERE tid=?", (tid, u64(ttid)))
            q("DELETE FROM ttrans WHERE tid=?", (tid,))
            if not obj:
                return
        ttid = u64(ttid)
        sql = " FROM tobj WHERE tid=?"
        data_id_list = [x for x, in q("SELECT data_id%s AND data_id IS NOT NULL"
                                      % sql, (ttid,))]
        q("INSERT INTO obj SELECT partition, oid, ?, data_id, value_tid" + sql,
          (tid, ttid))
        q("DELETE" + sql, (ttid,))
        self.releaseData(data_id_list)

    def abortTransaction(self, ttid):
        args = util.u64(ttid),
        q = self.query
        q("DELETE FROM tobj WHERE tid=?", args)
        q("DELETE FROM ttrans WHERE ttid=?", args)

    def deleteTransaction(self, tid):
        tid = util.u64(tid)
        q = self.query
        q("DELETE FROM pack WHERE tid=?", (tid,))
        q("DELETE FROM trans WHERE partition=? AND tid=?",
            (self._getPartition(tid), tid))

    def deleteObject(self, oid, serial=None):
        oid = util.u64(oid)
        sql = " FROM obj WHERE partition=? AND oid=?"
        args = [self._getPartition(oid), oid]
        if serial:
            sql += " AND tid=?"
            args.append(util.u64(serial))
        q = self.query
        data_id_list = [x for x, in q(
            "SELECT DISTINCT data_id%s AND data_id IS NOT NULL" % sql, args)]
        q("DELETE" + sql, args)
        self._pruneData(data_id_list)

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        sql = " WHERE partition=?"
        args = [partition]
        if min_tid is not None:
            sql += " AND ? < tid"
            args.append(min_tid)
        if max_tid is not None:
            sql += " AND tid <= ?"
            args.append(max_tid)
        q = self.query
        q("DELETE FROM pack WHERE tid IN ("
            "SELECT tid FROM trans JOIN pack USING (tid)%s)" % sql, args)
        q("DELETE FROM trans" + sql, args)
        sql = " FROM obj" + sql
        data_id_list = [x for x, in q(
            "SELECT DISTINCT data_id%s AND data_id IS NOT NULL" % sql, args)]
        q("DELETE" + sql, args)
        self._pruneData(data_id_list)

    def getTransaction(self, tid, all=False):
        tid = util.u64(tid)
        q = self.query
        r = q("SELECT trans.oids, user, description, ext, packed, ttid,"
                " approved, partial, pack.oids, pack_tid"
              " FROM trans LEFT JOIN pack USING (tid)"
              " WHERE partition=? AND tid=?",
              (self._getReadablePartition(tid), tid)).fetchone()
        if not r:
            if not all:
                return
            r = q("SELECT ttrans.oids, user, description, ext, packed, ttid,"
                    " approved, partial, pack.oids, pack_tid"
                  " FROM ttrans LEFT JOIN pack USING (tid)"
                  " WHERE tid=?", (tid,)).fetchone()
            if not r:
                return
        oids, user, desc, ext, packed, ttid, \
            approved, pack_partial, pack_oids, pack_tid = r
        return (
            splitOIDField(tid, oids),
            str(user), str(desc), str(ext),
            bool(packed), util.p64(ttid),
            None if pack_partial is None else (
                None if approved is None else bool(approved),
                bool(pack_partial),
                None if pack_oids is None else splitOIDField(tid, pack_oids),
                util.p64(pack_tid)))

    def _getObjectHistoryForUndo(self, oid, undo_tid):
        q = self.query
        args = self._getReadablePartition(oid), oid, undo_tid
        undo = q("SELECT tid FROM obj"
                 " WHERE partition=? AND oid=? AND tid<=?"
                 " ORDER BY tid DESC LIMIT 2", args).fetchall()
        if undo and undo.pop(0)[0] == undo_tid:
            return undo[0][0] if undo else None, q(
                "SELECT tid, value_tid FROM obj"
                " WHERE partition=? AND oid=? AND tid>?"
                " ORDER BY tid", args).fetchall()

    def getObjectHistoryWithLength(self, oid, offset, length):
        # FIXME: This method doesn't take client's current transaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        p64 = util.p64
        oid = util.u64(oid)
        return [(p64(tid), length or 0) for tid, length in self.query(
            "SELECT tid, LENGTH(value)"
            " FROM obj LEFT JOIN data ON obj.data_id = data.id"
            " WHERE partition=? AND oid=?"
            " ORDER BY tid DESC LIMIT ?,?",
            (self._getReadablePartition(oid), oid, offset, length))
            ] or None

    def _fetchObject(self, oid, tid):
        for serial, compression, checksum, data, value_serial in self.query(
                'SELECT tid, compression, data.hash, value, value_tid'
                ' FROM obj LEFT JOIN data ON obj.data_id = data.id'
                ' WHERE partition=? AND oid=? AND tid=?',
                (self._getReadablePartition(oid), oid, tid)):
            if checksum:
                checksum = str(checksum)
                data = str(data)
            return serial, compression, checksum, data, value_serial

    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        if max_tid < min_tid or length == 0:
            return ()
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        if not length:
            length = -1
        r = [(p64(serial), p64(oid)) for serial, oid in self.query(
                "SELECT tid, oid FROM obj"
                " WHERE partition=? AND tid=? AND ?<=oid"
                " ORDER BY tid ASC, oid ASC LIMIT ?",
                (partition, min_tid, u64(min_oid), length))]
        if length > 0:
            length -= len(r)
            if not length:
                return r
        for serial, oid in self.query(
                "SELECT tid, oid FROM obj"
                " WHERE partition=? AND ?<tid AND tid<=?"
                " ORDER BY tid ASC, oid ASC LIMIT ?",
                (partition, min_tid, u64(max_tid), length)):
            r.append((p64(serial), p64(oid)))
        return r

    def _getTIDList(self, offset, length, partition_list):
        return (t[0] for t in self.query(
            "SELECT tid FROM trans WHERE `partition` in (%s)"
            " ORDER BY tid DESC LIMIT %d,%d"
            % (','.join(map(str, partition_list)), offset, length)))

    def oidsFrom(self, partition, length, min_oid, tid):
        if partition not in self._readable_set:
            raise NonReadableCell
        p64 = util.p64
        u64 = util.u64
        r = self.query("SELECT oid, data_id FROM obj JOIN ("
                "SELECT `partition`, oid, MAX(tid) AS tid FROM obj"
                " WHERE partition=? AND oid>=? AND tid<=?"
                " GROUP BY oid ORDER BY oid LIMIT ?"
                ") AS t USING (`partition`, oid, tid)",
            (partition, u64(min_oid), u64(tid), length)).fetchall()
        return None if len(r) < length else p64(r[-1][0] + self.np), \
            [p64(oid) for oid, data_id in r if data_id is not None]

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        max_tid = u64(max_tid)
        if length is None:
            length = -1
        return [p64(t[0]) for t in self.query("""\
            SELECT tid FROM trans
            WHERE partition=? AND ?<=tid AND tid<=?
            ORDER BY tid ASC LIMIT ?""",
            (partition, min_tid, max_tid, length))]

    _pack = " FROM obj WHERE partition=? AND oid=? AND tid<?"
    def _pack(self, offset, oid, tid, limit=None,
            _select_data_id_sql="SELECT DISTINCT data_id" + _pack,
            _delete_obj_sql="DELETE" + _pack):
        q = self.query
        data_id_set = set()
        value_dict = defaultdict(list)
        sql = ("SELECT COUNT(*), oid, MAX(tid) FROM obj"
               " WHERE partition=%s AND tid<=%s AND oid%s GROUP BY oid%s") % (
            offset, tid,
            ">=%s" % oid if limit else " IN (%s)" % ','.join(map(str, oid)),
            " ORDER BY oid LIMIT %s" % limit if limit else "")
        oid = None
        for x, oid, max_tid in q(sql):
            for x in q("SELECT tid + (data_id IS NULL) FROM obj"
                  " WHERE partition=? AND oid=? AND tid=?"
                    " AND (data_id IS NULL OR ?>1)",
                  (offset, oid, max_tid, x)):
                x = (offset, oid) + x
                data_id_set.update(*zip(*q(_select_data_id_sql, x)))
                q(_delete_obj_sql, x)
                break
        data_id_set.discard(None)
        self._pruneData(data_id_set)
        return limit and oid, len(data_id_set)

    def checkTIDRange(self, partition, length, min_tid, max_tid):
        # XXX: SQLite's GROUP_CONCAT is slow (looks like quadratic)
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
        # We don't ask SQLite to compute everything (like in checkTIDRange)
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

    def dump(self):
        main = []
        data = []
        for line in self.conn.iterdump():
            if line.startswith('INSERT '):
                assert line.endswith(';'), line
                data.append(line)
                continue
            if line.startswith('CREATE TABLE '):
                # ALTER TABLE adds quotes.
                create, table, name, tail = line.split(' ', 3)
                line = ' '.join((create, table, name.strip('"'), tail))
            main.append(line)
        assert line == 'COMMIT;', line
        data.sort()
        main[-1:-1] = data
        return '\n'.join(main) + '\n'

    def _restore(self, sql):
        self.conn.executescript(sql)
