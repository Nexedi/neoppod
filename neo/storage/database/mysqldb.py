#
# Copyright (C) 2006-2015  Nexedi SA
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

from binascii import a2b_hex
import MySQLdb
from MySQLdb import DataError, IntegrityError, OperationalError
from MySQLdb.constants.CR import SERVER_GONE_ERROR, SERVER_LOST
from MySQLdb.constants.ER import DATA_TOO_LONG, DUP_ENTRY
from array import array
from hashlib import sha1
import os
import re
import string
import struct
import time

from . import DatabaseManager, LOG_QUERIES
from .manager import CreationUndone, splitOIDField
from neo.lib import logging, util
from neo.lib.exception import DatabaseFailure
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID, ZERO_HASH


def getPrintableQuery(query, max=70):
    return ''.join(c if c in string.printable and c not in '\t\x0b\x0c\r'
        else '\\x%02x' % ord(c) for c in query)


class MySQLDatabaseManager(DatabaseManager):
    """This class manages a database on MySQL."""

    ENGINES = "InnoDB", "TokuDB"
    _engine = ENGINES[0] # default engine

    # Disabled even on MySQL 5.1-5.5 and MariaDB 5.2-5.3 because
    # 'select count(*) from obj' sometimes returns incorrect values
    # (tested with testOudatedCellsOnDownStorage).
    _use_partition = False

    def __init__(self, *args, **kw):
        super(MySQLDatabaseManager, self).__init__(*args, **kw)
        self.conn = None
        self._config = {}
        self._connect()

    def _parse(self, database):
        """ Get the database credentials (username, password, database) """
        # expected pattern : [user[:password]@]database[(~|.|/)unix_socket]
        self.user, self.passwd, self.db, self.socket = re.match(
            '(?:([^:]+)(?::(.*))?@)?([^~./]+)(.+)?$', database).groups()

    def close(self):
        self.conn.close()

    def _connect(self):
        kwd = {'db' : self.db, 'user' : self.user}
        if self.passwd is not None:
            kwd['passwd'] = self.passwd
        if self.socket:
            kwd['unix_socket'] = os.path.expanduser(self.socket)
        logging.info('connecting to MySQL on the database %s with user %s',
                     self.db, self.user)
        if self._wait < 0:
            timeout_at = None
        else:
            timeout_at = time.time() + self._wait
        while True:
            try:
                self.conn = MySQLdb.connect(**kwd)
                break
            except Exception:
                if timeout_at is not None and time.time() >= timeout_at:
                    raise
                logging.exception('Connection to MySQL failed, retrying.')
                time.sleep(1)
        self._active = 0
        self.conn.autocommit(False)
        self.conn.query("SET SESSION group_concat_max_len = %u" % (2**32-1))
        self.conn.set_sql_mode("TRADITIONAL,NO_ENGINE_SUBSTITUTION")

    def commit(self):
        logging.debug('committing...')
        self.conn.commit()
        self._active = 0

    def query(self, query):
        """Query data from a database."""
        if LOG_QUERIES:
            logging.debug('querying %s...',
                getPrintableQuery(query.split('\n', 1)[0][:70]))
        while 1:
            conn = self.conn
            try:
                conn.query(query)
                if query.startswith("SELECT "):
                    r = conn.store_result()
                    return tuple([
                        tuple([d.tostring() if isinstance(d, array) else d
                              for d in row])
                        for row in r.fetch_row(r.num_rows())])
                break
            except OperationalError, m:
                if self._active or m[0] not in (SERVER_GONE_ERROR, SERVER_LOST):
                    raise DatabaseFailure('MySQL error %d: %s\nQuery: %s'
                        % (m[0], m[1], getPrintableQuery(query[:1000])))
                logging.info('the MySQL server is gone; reconnecting')
                self._connect()
        r = query.split(None, 1)[0]
        if r in ("INSERT", "REPLACE", "DELETE", "UPDATE"):
            self._active = 1
        else:
            assert r in ("ALTER", "CREATE", "DROP", "TRUNCATE"), query

    @property
    def escape(self):
        """Escape special characters in a string."""
        return self.conn.escape_string

    def erase(self):
        self.query(
            "DROP TABLE IF EXISTS config, pt, trans, obj, data, ttrans, tobj")

    def _setup(self):
        self._config.clear()
        q = self.query
        p = engine = self._engine
        # The table "config" stores configuration parameters which affect the
        # persistent data.
        q("""CREATE TABLE IF NOT EXISTS config (
                 name VARBINARY(255) NOT NULL PRIMARY KEY,
                 value VARBINARY(255) NULL
             ) ENGINE=""" + engine)

        # The table "pt" stores a partition table.
        q("""CREATE TABLE IF NOT EXISTS pt (
                 rid INT UNSIGNED NOT NULL,
                 nid INT NOT NULL,
                 state TINYINT UNSIGNED NOT NULL,
                 PRIMARY KEY (rid, nid)
             ) ENGINE=""" + engine)

        if self._use_partition:
            p += """ PARTITION BY LIST (`partition`) (
                PARTITION dummy VALUES IN (NULL))"""

        # The table "trans" stores information on committed transactions.
        q("""CREATE TABLE IF NOT EXISTS trans (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid BIGINT UNSIGNED NOT NULL,
                 PRIMARY KEY (`partition`, tid)
             ) ENGINE=""" + p)

        # The table "obj" stores committed object metadata.
        q("""CREATE TABLE IF NOT EXISTS obj (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 oid BIGINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 data_id BIGINT UNSIGNED NULL,
                 value_tid BIGINT UNSIGNED NULL,
                 PRIMARY KEY (`partition`, tid, oid),
                 KEY (`partition`, oid, tid),
                 KEY (data_id)
             ) ENGINE=""" + p)

        if engine == "TokuDB":
            engine += " compression='tokudb_uncompressed'"

        # The table "data" stores object data.
        # We'd like to have partial index on 'hash' colum (e.g. hash(4))
        # but 'UNIQUE' constraint would not work as expected.
        q("""CREATE TABLE IF NOT EXISTS data (
                 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                 hash BINARY(20) NOT NULL,
                 compression TINYINT UNSIGNED NULL,
                 value MEDIUMBLOB NOT NULL,
                 UNIQUE (hash, compression)
             ) ENGINE=""" + engine)

        q("""CREATE TABLE IF NOT EXISTS bigdata (
                 id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                 value MEDIUMBLOB NOT NULL
             ) ENGINE=""" + engine)

        # The table "ttrans" stores information on uncommitted transactions.
        q("""CREATE TABLE IF NOT EXISTS ttrans (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid BIGINT UNSIGNED NOT NULL
             ) ENGINE=""" + engine)

        # The table "tobj" stores uncommitted object metadata.
        q("""CREATE TABLE IF NOT EXISTS tobj (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 oid BIGINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 data_id BIGINT UNSIGNED NULL,
                 value_tid BIGINT UNSIGNED NULL,
                 PRIMARY KEY (tid, oid)
             ) ENGINE=""" + engine)

        self._uncommitted_data.update(q("SELECT data_id, count(*)"
            " FROM tobj WHERE data_id IS NOT NULL GROUP BY data_id"))

    def getConfiguration(self, key):
        try:
            return self._config[key]
        except KeyError:
            sql_key = self.escape(str(key))
            try:
                r = self.query("SELECT value FROM config WHERE name = '%s'"
                               % sql_key)[0][0]
            except IndexError:
                r = None
            self._config[key] = r
            return r

    def _setConfiguration(self, key, value):
        q = self.query
        e = self.escape
        self._config[key] = value
        k = e(str(key))
        if value is None:
            q("DELETE FROM config WHERE name = '%s'" % k)
            return
        value = str(value)
        sql = "REPLACE INTO config VALUES ('%s', '%s')" % (k, e(value))
        try:
            q(sql)
        except DataError, (code, _):
            if code != DATA_TOO_LONG or len(value) < 256 or key != "zodb":
                raise
            q("ALTER TABLE config MODIFY value VARBINARY(%s) NULL" % len(value))
            q(sql)

    def getPartitionTable(self):
        return self.query("SELECT * FROM pt")

    def getLastTID(self, max_tid):
        return self.query("SELECT MAX(tid) FROM trans WHERE tid<=%s"
                          % max_tid)[0][0]

    def _getLastIDs(self, all=True):
        p64 = util.p64
        q = self.query
        trans = {partition: p64(tid)
            for partition, tid in q("SELECT `partition`, MAX(tid)"
                                    " FROM trans GROUP BY `partition`")}
        obj = {partition: p64(tid)
            for partition, tid in q("SELECT `partition`, MAX(tid)"
                                    " FROM obj GROUP BY `partition`")}
        oid = q("SELECT MAX(oid) FROM (SELECT MAX(oid) AS oid FROM obj"
                                      " GROUP BY `partition`) as t")[0][0]
        if all:
            tid = q("SELECT MAX(tid) FROM ttrans")[0][0]
            if tid is not None:
                trans[None] = p64(tid)
            tid, toid = q("SELECT MAX(tid), MAX(oid) FROM tobj")[0]
            if tid is not None:
                obj[None] = p64(tid)
            if toid is not None and (oid < toid or oid is None):
                oid = toid
        return trans, obj, None if oid is None else p64(oid)

    def getUnfinishedTIDList(self):
        p64 = util.p64
        return [p64(t[0]) for t in self.query("SELECT tid FROM ttrans"
                                       " UNION SELECT tid FROM tobj")]

    def objectPresent(self, oid, tid, all = True):
        oid = util.u64(oid)
        tid = util.u64(tid)
        q = self.query
        return q("SELECT 1 FROM obj WHERE `partition`=%d AND oid=%d AND tid=%d"
                 % (self._getPartition(oid), oid, tid)) or all and \
               q("SELECT 1 FROM tobj WHERE tid=%d AND oid=%d" % (tid, oid))

    def getLastObjectTID(self, oid):
        oid = util.u64(oid)
        r = self.query("SELECT tid FROM obj"
                       " WHERE `partition`=%d AND oid=%d"
                       " ORDER BY tid DESC LIMIT 1"
                       % (self._getPartition(oid), oid))
        return util.p64(r[0][0]) if r else None

    def _getNextTID(self, *args): # partition, oid, tid
        r = self.query("SELECT tid FROM obj"
                       " WHERE `partition`=%d AND oid=%d AND tid>%d"
                       " ORDER BY tid LIMIT 1" % args)
        return r[0][0] if r else None

    def _getObject(self, oid, tid=None, before_tid=None):
        q = self.query
        partition = self._getPartition(oid)
        sql = ('SELECT tid, compression, data.hash, value, value_tid'
               ' FROM obj LEFT JOIN data ON (obj.data_id = data.id)'
               ' WHERE `partition` = %d AND oid = %d') % (partition, oid)
        if before_tid is not None:
            sql += ' AND tid < %d ORDER BY tid DESC LIMIT 1' % before_tid
        elif tid is not None:
            sql += ' AND tid = %d' % tid
        else:
            # XXX I want to express "HAVING tid = MAX(tid)", but
            # MySQL does not use an index for a HAVING clause!
            sql += ' ORDER BY tid DESC LIMIT 1'
        r = q(sql)
        try:
            serial, compression, checksum, data, value_serial = r[0]
        except IndexError:
            return None
        if compression and compression & 0x80:
            compression &= 0x7f
            data = ''.join(self._bigData(data))
        return (serial, self._getNextTID(partition, oid, serial),
                compression, checksum, data, value_serial)

    def changePartitionTable(self, ptid, cell_list, reset=False):
        offset_list = []
        q = self.query
        if reset:
            q("TRUNCATE pt")
        for offset, nid, state in cell_list:
            # TODO: this logic should move out of database manager
            # add 'dropCells(cell_list)' to API and use one query
            if state == CellStates.DISCARDED:
                q("DELETE FROM pt WHERE rid = %d AND nid = %d"
                  % (offset, nid))
            else:
                offset_list.append(offset)
                q("INSERT INTO pt VALUES (%d, %d, %d)"
                  " ON DUPLICATE KEY UPDATE state = %d"
                  % (offset, nid, state, state))
        self.setPTID(ptid)
        if self._use_partition:
            for offset in offset_list:
                add = """ALTER TABLE %%s ADD PARTITION (
                    PARTITION p%u VALUES IN (%u))""" % (offset, offset)
                for table in 'trans', 'obj':
                    try:
                        self.conn.query(add % table)
                    except OperationalError, (code, _):
                        if code != 1517: # duplicate partition name
                            raise

    def dropPartitions(self, offset_list):
        q = self.query
        # XXX: these queries are inefficient (execution time increase with
        # row count, although we use indexes) when there are rows to
        # delete. It should be done as an idle task, by chunks.
        for partition in offset_list:
            where = " WHERE `partition`=%d" % partition
            data_id_list = [x for x, in
                q("SELECT DISTINCT data_id FROM obj" + where) if x]
            if not self._use_partition:
                q("DELETE FROM obj" + where)
                q("DELETE FROM trans" + where)
            self._pruneData(data_id_list)
        if self._use_partition:
            drop = "ALTER TABLE %s DROP PARTITION" + \
                ','.join(' p%u' % i for i in offset_list)
            for table in 'trans', 'obj':
                try:
                    self.conn.query(drop % table)
                except OperationalError, (code, _):
                    if code != 1508: # already dropped
                        raise

    def dropUnfinishedData(self):
        q = self.query
        data_id_list = [x for x, in q("SELECT data_id FROM tobj") if x]
        q("TRUNCATE tobj")
        q("TRUNCATE ttrans")
        self.releaseData(data_id_list, True)

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        e = self.escape
        u64 = util.u64
        tid = u64(tid)
        if temporary:
            obj_table = 'tobj'
            trans_table = 'ttrans'
        else:
            obj_table = 'obj'
            trans_table = 'trans'
        q = self.query
        for oid, data_id, value_serial in object_list:
            oid = u64(oid)
            partition = self._getPartition(oid)
            if value_serial:
                value_serial = u64(value_serial)
                (data_id,), = q("SELECT data_id FROM obj"
                    " WHERE `partition`=%d AND oid=%d AND tid=%d"
                    % (partition, oid, value_serial))
                if temporary:
                    self.holdData(data_id)
            else:
                value_serial = 'NULL'
            q("REPLACE INTO %s VALUES (%d, %d, %d, %s, %s)" % (obj_table,
                partition, oid, tid, data_id or 'NULL', value_serial))
        if transaction:
            oid_list, user, desc, ext, packed, ttid = transaction
            partition = self._getPartition(tid)
            assert packed in (0, 1)
            q("REPLACE INTO %s VALUES (%d,%d,%i,'%s','%s','%s','%s',%d)" % (
                trans_table, partition, tid, packed, e(''.join(oid_list)),
                e(user), e(desc), e(ext), u64(ttid)))
        if temporary:
            self.commit()

    _structLL = struct.Struct(">LL")
    _unpackLL = _structLL.unpack

    def _pruneData(self, data_id_list):
        data_id_list = set(data_id_list).difference(self._uncommitted_data)
        if data_id_list:
            q = self.query
            id_list = []
            bigid_list = []
            for id, value in q("SELECT id, IF(compression < 128, NULL, value)"
                               " FROM data LEFT JOIN obj ON (id = data_id)"
                               " WHERE id IN (%s) AND data_id IS NULL"
                               % ",".join(map(str, data_id_list))):
                id_list.append(str(id))
                if value:
                    bigdata_id, length = self._unpackLL(value)
                    bigid_list += xrange(bigdata_id,
                                         bigdata_id + (length + 0x7fffff >> 23))
            if id_list:
                q("DELETE FROM data WHERE id IN (%s)" % ",".join(id_list))
                if bigid_list:
                    q("DELETE FROM bigdata WHERE id IN (%s)"
                      % ",".join(map(str, bigid_list)))

    def _bigData(self, value):
        bigdata_id, length = self._unpackLL(value)
        q = self.query
        return (q("SELECT value FROM bigdata WHERE id=%s" % i)[0][0]
            for i in xrange(bigdata_id,
                            bigdata_id + (length + 0x7fffff >> 23)))

    def storeData(self, checksum, data, compression, _pack=_structLL.pack):
        e = self.escape
        checksum = e(checksum)
        if 0x1000000 <= len(data): # 16M (MEDIUMBLOB limit)
            compression |= 0x80
            q = self.query
            for r, d in q("SELECT id, value FROM data"
                          " WHERE hash='%s' AND compression=%s"
                          % (checksum, compression)):
                i = 0
                for d in self._bigData(d):
                    j = i + len(d)
                    if data[i:j] != d:
                        raise IntegrityError(DUP_ENTRY)
                    i = j
                if j != len(data):
                    raise IntegrityError(DUP_ENTRY)
                return r
            i = 'NULL'
            length = len(data)
            for j in xrange(0, length, 0x800000): # 8M
                q("INSERT INTO bigdata VALUES (%s, '%s')"
                  % (i, e(data[j:j+0x800000])))
                if not j:
                    i = bigdata_id = self.conn.insert_id()
                i += 1
            data = _pack(bigdata_id, length)
        try:
            self.query("INSERT INTO data VALUES (NULL, '%s', %d, '%s')" %
                       (checksum, compression,  e(data)))
        except IntegrityError, (code, _):
            if code == DUP_ENTRY:
                (r, d), = self.query("SELECT id, value FROM data"
                                     " WHERE hash='%s' AND compression=%s"
                                     % (checksum, compression))
                if d == data:
                    return r
            raise
        return self.conn.insert_id()

    del _structLL

    def _getDataTID(self, oid, tid=None, before_tid=None):
        sql = ('SELECT tid, value_tid FROM obj'
               ' WHERE `partition` = %d AND oid = %d'
              ) % (self._getPartition(oid), oid)
        if tid is not None:
            sql += ' AND tid = %d' % tid
        elif before_tid is not None:
            sql += ' AND tid < %d ORDER BY tid DESC LIMIT 1' % before_tid
        else:
            # XXX I want to express "HAVING tid = MAX(tid)", but
            # MySQL does not use an index for a HAVING clause!
            sql += ' ORDER BY tid DESC LIMIT 1'
        r = self.query(sql)
        return r[0] if r else (None, None)

    def finishTransaction(self, tid):
        q = self.query
        tid = util.u64(tid)
        sql = " FROM tobj WHERE tid=%d" % tid
        data_id_list = [x for x, in q("SELECT data_id" + sql) if x]
        q("INSERT INTO obj SELECT *" + sql)
        q("DELETE FROM tobj WHERE tid=%d" % tid)
        q("INSERT INTO trans SELECT * FROM ttrans WHERE tid=%d" % tid)
        q("DELETE FROM ttrans WHERE tid=%d" % tid)
        self.releaseData(data_id_list)
        self.commit()

    def deleteTransaction(self, tid, oid_list=()):
        u64 = util.u64
        tid = u64(tid)
        getPartition = self._getPartition
        q = self.query
        sql = " FROM tobj WHERE tid=%d" % tid
        data_id_list = [x for x, in q("SELECT data_id" + sql) if x]
        self.releaseData(data_id_list)
        q("DELETE" + sql)
        q("""DELETE FROM ttrans WHERE tid = %d""" % tid)
        q("""DELETE FROM trans WHERE `partition` = %d AND tid = %d""" %
            (getPartition(tid), tid))
        # delete from obj using indexes
        data_id_set = set()
        for oid in oid_list:
            oid = u64(oid)
            sql = " FROM obj WHERE `partition`=%d AND oid=%d AND tid=%d" \
               % (getPartition(oid), oid, tid)
            data_id_set.update(*q("SELECT data_id" + sql))
            q("DELETE" + sql)
        data_id_set.discard(None)
        self._pruneData(data_id_set)

    def deleteObject(self, oid, serial=None):
        u64 = util.u64
        oid = u64(oid)
        sql = " FROM obj WHERE `partition`=%d AND oid=%d" \
            % (self._getPartition(oid), oid)
        if serial:
            sql += ' AND tid=%d' % u64(serial)
        q = self.query
        data_id_list = [x for x, in q("SELECT DISTINCT data_id" + sql) if x]
        q("DELETE" + sql)
        self._pruneData(data_id_list)

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        sql = " WHERE `partition`=%d" % partition
        if min_tid:
            sql += " AND %d < tid" % util.u64(min_tid)
        if max_tid:
            sql += " AND tid <= %d" % util.u64(max_tid)
        q = self.query
        q("DELETE FROM trans" + sql)
        sql = " FROM obj" + sql
        data_id_list = [x for x, in q("SELECT DISTINCT data_id" + sql) if x]
        q("DELETE" + sql)
        self._pruneData(data_id_list)

    def getTransaction(self, tid, all = False):
        tid = util.u64(tid)
        q = self.query
        r = q("SELECT oids, user, description, ext, packed, ttid"
              " FROM trans WHERE `partition` = %d AND tid = %d"
              % (self._getPartition(tid), tid))
        if not r and all:
            r = q("SELECT oids, user, description, ext, packed, ttid"
                  " FROM ttrans WHERE tid = %d" % tid)
        if r:
            oids, user, desc, ext, packed, ttid = r[0]
            oid_list = splitOIDField(tid, oids)
            return oid_list, user, desc, ext, bool(packed), util.p64(ttid)

    def getObjectHistory(self, oid, offset, length):
        # FIXME: This method doesn't take client's current ransaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        oid = util.u64(oid)
        p64 = util.p64
        r = self.query("SELECT tid, IF(compression < 128, LENGTH(value),"
            "  CAST(CONV(HEX(SUBSTR(value, 5, 4)), 16, 10) AS INT))"
            " FROM obj LEFT JOIN data ON (obj.data_id = data.id)"
            " WHERE `partition` = %d AND oid = %d AND tid >= %d"
            " ORDER BY tid DESC LIMIT %d, %d" %
            (self._getPartition(oid), oid, self._getPackTID(), offset, length))
        if r:
            return [(p64(tid), length or 0) for tid, length in r]

    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        r = self.query('SELECT tid, oid FROM obj'
                       ' WHERE `partition` = %d AND tid <= %d'
                       ' AND (tid = %d AND %d <= oid OR %d < tid)'
                       ' ORDER BY tid ASC, oid ASC LIMIT %d' % (
            partition, u64(max_tid), min_tid, u64(min_oid), min_tid, length))
        return [(p64(serial), p64(oid)) for serial, oid in r]

    def getTIDList(self, offset, length, partition_list):
        q = self.query
        r = q("""SELECT tid FROM trans WHERE `partition` in (%s)
                    ORDER BY tid DESC LIMIT %d,%d""" \
                % (','.join(map(str, partition_list)), offset, length))
        return [util.p64(t[0]) for t in r]

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        max_tid = u64(max_tid)
        r = self.query("""SELECT tid FROM trans
                    WHERE `partition` = %(partition)d
                    AND tid >= %(min_tid)d AND tid <= %(max_tid)d
                    ORDER BY tid ASC LIMIT %(length)d""" % {
            'partition': partition,
            'min_tid': min_tid,
            'max_tid': max_tid,
            'length': length,
        })
        return [p64(t[0]) for t in r]

    def _updatePackFuture(self, oid, orig_serial, max_serial):
        q = self.query
        # Before deleting this objects revision, see if there is any
        # transaction referencing its value at max_serial or above.
        # If there is, copy value to the first future transaction. Any further
        # reference is just updated to point to the new data location.
        value_serial = None
        kw = {
          'partition': self._getPartition(oid),
          'oid': oid,
          'orig_tid': orig_serial,
          'max_tid': max_serial,
          'new_tid': 'NULL',
        }
        for kw['table'] in 'obj', 'tobj':
            for kw['tid'], in q('SELECT tid FROM %(table)s'
                  ' WHERE `partition`=%(partition)d AND oid=%(oid)d'
                  ' AND tid>=%(max_tid)d AND value_tid=%(orig_tid)d'
                  ' ORDER BY tid ASC' % kw):
                q('UPDATE %(table)s SET value_tid=%(new_tid)s'
                  ' WHERE `partition`=%(partition)d AND oid=%(oid)d'
                  ' AND tid=%(tid)d' % kw)
                if value_serial is None:
                    # First found, mark its serial for future reference.
                    kw['new_tid'] = value_serial = kw['tid']
        return value_serial

    def pack(self, tid, updateObjectDataForPack):
        # TODO: unit test (along with updatePackFuture)
        p64 = util.p64
        tid = util.u64(tid)
        updatePackFuture = self._updatePackFuture
        getPartition = self._getPartition
        q = self.query
        self._setPackTID(tid)
        for count, oid, max_serial in q("SELECT COUNT(*) - 1, oid, MAX(tid)"
                                        " FROM obj WHERE tid <= %d GROUP BY oid"
                                        % tid):
            partition = getPartition(oid)
            if q("SELECT 1 FROM obj WHERE `partition` = %d"
                 " AND oid = %d AND tid = %d AND data_id IS NULL"
                 % (partition, oid, max_serial)):
                max_serial += 1
            elif not count:
                continue
            # There are things to delete for this object
            data_id_set = set()
            sql = ' FROM obj WHERE `partition`=%d AND oid=%d' \
                ' AND tid<%d' % (partition, oid, max_serial)
            for serial, data_id in q('SELECT tid, data_id' + sql):
                data_id_set.add(data_id)
                new_serial = updatePackFuture(oid, serial, max_serial)
                if new_serial:
                    new_serial = p64(new_serial)
                updateObjectDataForPack(p64(oid), p64(serial),
                                        new_serial, data_id)
            q('DELETE' + sql)
            data_id_set.discard(None)
            self._pruneData(data_id_set)
        self.commit()

    def checkTIDRange(self, partition, length, min_tid, max_tid):
        count, tid_checksum, max_tid = self.query(
            """SELECT COUNT(*), SHA1(GROUP_CONCAT(tid SEPARATOR ",")), MAX(tid)
               FROM (SELECT tid FROM trans
                     WHERE `partition` = %(partition)s
                       AND tid >= %(min_tid)d
                       AND tid <= %(max_tid)d
                     ORDER BY tid ASC %(limit)s) AS t""" % {
            'partition': partition,
            'min_tid': util.u64(min_tid),
            'max_tid': util.u64(max_tid),
            'limit': '' if length is None else 'LIMIT %u' % length,
        })[0]
        if count:
            return count, a2b_hex(tid_checksum), util.p64(max_tid)
        return 0, ZERO_HASH, ZERO_TID

    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        u64 = util.u64
        # We don't ask MySQL to compute everything (like in checkTIDRange)
        # because it's difficult to get the last serial _for the last oid_.
        # We would need a function (that could be named 'LAST') that returns the
        # last grouped value, instead of the greatest one.
        r = self.query(
            """SELECT tid, oid
               FROM obj
               WHERE `partition` = %(partition)s
                 AND tid <= %(max_tid)d
                 AND (tid > %(min_tid)d OR
                      tid = %(min_tid)d AND oid >= %(min_oid)d)
               ORDER BY tid, oid %(limit)s""" % {
            'min_oid': u64(min_oid),
            'min_tid': u64(min_tid),
            'max_tid': u64(max_tid),
            'limit': '' if length is None else 'LIMIT %u' % length,
            'partition': partition,
        })
        if r:
            p64 = util.p64
            return (len(r),
                    sha1(','.join(str(x[0]) for x in r)).digest(),
                    p64(r[-1][0]),
                    sha1(','.join(str(x[1]) for x in r)).digest(),
                    p64(r[-1][1]))
        return 0, ZERO_HASH, ZERO_TID, ZERO_HASH, ZERO_OID
