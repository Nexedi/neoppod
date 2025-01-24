#
# Copyright (C) 2006-2019  Nexedi SA
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

import os, re, string, struct, sys, time, weakref
from binascii import a2b_hex
from collections import defaultdict, OrderedDict
from functools import wraps
from hashlib import sha1
from neo import *
from . import useMySQLdb
if useMySQLdb():
    binding_name = 'MySQLdb'
    from MySQLdb.connections import Connection
    from MySQLdb.converters import string_literal
    from MySQLdb import DataError, \
        IntegrityError, OperationalError, ProgrammingError
    InternalOrOperationalError = OperationalError
    from MySQLdb.constants.CR import SERVER_GONE_ERROR, SERVER_LOST
    from MySQLdb.constants.ER import DATA_TOO_LONG, DUP_ENTRY, NO_SUCH_TABLE
    from MySQLdb.release import __version__ as binding_version
    def fetch_all(conn):
        return conn.store_result().fetch_row(0)
    # for tests
    from MySQLdb import NotSupportedError
    from MySQLdb.constants.ER import BAD_DB_ERROR, UNKNOWN_STORAGE_ENGINE
else:
    binding_name = 'PyMySQL'
    from pymysql.connections import Connection
    if six.PY3:
        from pymysql.converters import _escape_table
        string_literal = lambda x: b"'%s'" % (
            x.decode("ascii", "surrogateescape") if type(x) is bytes else
            x).translate(_escape_table).encode("ascii", "surrogateescape")
    else:
        from pymysql.converters import escape_string
        string_literal = lambda x: b"'%s'" % escape_string(x)
    from pymysql import __version__ as binding_version, DataError, \
        IntegrityError, InternalError, OperationalError, ProgrammingError
    InternalOrOperationalError = InternalError, OperationalError
    from pymysql.constants.CR import (
        CR_SERVER_GONE_ERROR as SERVER_GONE_ERROR,
        CR_SERVER_LOST as SERVER_LOST)
    from pymysql.constants.ER import DATA_TOO_LONG, DUP_ENTRY, NO_SUCH_TABLE
    def fetch_all(conn):
        return conn._result.rows
    # for tests
    from pymysql import NotSupportedError
    from pymysql.constants.ER import BAD_DB_ERROR, UNKNOWN_STORAGE_ENGINE
from . import LOG_QUERIES, DatabaseFailure
from .manager import MVCCDatabaseManager, splitOIDField
from neo.lib import logging, util
from neo.lib.exception import NonReadableCell, UndoPackError
from neo.lib.interfaces import implements
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID, ZERO_HASH, MAX_TID


class MysqlError(DatabaseFailure):

    def __init__(self, exc, query=None):
        self.exc = exc
        self.query = query
        self.transient_failure = exc.args[0] in (SERVER_GONE_ERROR, SERVER_LOST)

    code = property(lambda self: self.exc.args[0])

    def __str__(self):
        msg = 'MySQL error %s: %s' % self.exc.args
        return msg if self.query is None else '%s\nQuery: %s' % (
            msg, getPrintableQuery(self.query[:1000]))

    def logTransientFailure(self):
        logging.info('the MySQL server is gone; reconnecting')


def getPrintableQuery(query, max=70):
    return ''.join(c if c in string.printable and c not in '\t\x0b\x0c\r'
        else '\\x%02x' % ord(c) for c in (
            query.decode('ascii', 'backslashreplace') if six.PY3 else query))

def auto_reconnect(wrapped):
    def wrapper(self, *args):
        # Try 3 times at most. When it fails too often for the same
        # query then the disconnection is likely caused by this query.
        # We don't want to enter into an infinite loop.
        retry = 2
        while 1:
            try:
                return wrapped(self, *args)
            except InternalOrOperationalError as m:
                # IDEA: Is it safe to retry in case of DISK_FULL ?
                # XXX:  However, this would another case of failure that would
                #       be unnoticed by other nodes (ADMIN & MASTER). When
                #       there are replicas, it may be preferred to not retry.
                e = MysqlError(m, *args)
                if self._active or not (e.transient_failure and retry):
                    if __debug__:
                        e.getFailingDatabaseManager = weakref.ref(self)
                    raise e
                e.logTransientFailure()
                assert not self._deferred_commit
                self.close()
                retry -= 1
    return wraps(wrapped)(wrapper)

def splitList(x, n):
    for i in range(0, len(x), n):
        yield x[i:i+n]


@implements
class MySQLDatabaseManager(MVCCDatabaseManager):
    """This class manages a database on MySQL."""

    VERSION = 4
    ENGINES = "InnoDB", "RocksDB"
    _engine = ENGINES[0] # default engine

    _max_allowed_packet = 32769 * 1024

    def _parse(self, database):
        """ Get the database credentials (username, password, database) """
        # expected pattern : [user[:password]@]database[(~|.|/)unix_socket]
        self.user, self.passwd, self.db, self.socket = re.match(
            '(?:([^:]+)(?::(.*))?@)?([^~./]+)(.+)?$', database).groups()

    def _close(self):
        try:
            conn = self.__dict__.pop('conn')
        except KeyError:
            return
        conn.close()

    def __getattr__(self, attr):
        if attr == 'conn':
            self._tryConnect()
        return super(MySQLDatabaseManager, self).__getattr__(attr)

    def _tryConnect(self):
        # BBB: db/passwd are deprecated favour of database/password since 1.3.8
        kwd = {'db' : self.db}
        if self.user:
            kwd['user'] = self.user
            if self.passwd is not None:
                kwd['passwd'] = self.passwd
        if self.socket:
            kwd['unix_socket'] = os.path.expanduser(self.socket)
        logging.info('Using %s %s to connect to the database %s with user %s',
                     binding_name, binding_version, self.db, self.user)
        self._active = 0
        if self._wait < 0:
            timeout_at = None
        else:
            timeout_at = time.time() + self._wait
        last = None
        while True:
            try:
                self.conn = Connection(**kwd)
                break
            except Exception as e:
                if None is not timeout_at <= time.time():
                    raise
                e = str(e)
                if last == e:
                    log = logging.debug
                else:
                    last = e
                    log = logging.exception
                log('Connection to MySQL failed, retrying.')
                time.sleep(1)
        self._config = {}
        conn = self.conn
        conn.autocommit(False)
        conn.query("SET"
            " SESSION wait_timeout = 2147483," # we'd like to disable it
            " SESSION sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION',"
            " SESSION group_concat_max_len = %u" % (2**32-1))
        if self._engine == 'RocksDB':
            # Maximum value for _deleteRange.
            conn.query("SET SESSION rocksdb_max_row_locks = %u" % 2**30)
        def query(sql):
            conn.query(sql)
            return fetch_all(conn)
        if self.LOCK:
            (locked,), = query("SELECT GET_LOCK('%s.%s', 0)"
                % (self.db, self.LOCK))
            if not locked:
                sys.exit(self.LOCKED)
        (name, value), = query(
            "SHOW VARIABLES WHERE variable_name='max_allowed_packet'")
        if int(value) < self._max_allowed_packet:
            raise DatabaseFailure("Global variable %r is too small."
                " Minimal value must be %uk."
                % (name, self._max_allowed_packet // 1024))
        self._max_allowed_packet = int(value)
        try:
            self._dedup = bool(query(
                "SHOW INDEX FROM data WHERE key_name='hash'"))
        except ProgrammingError as e:
            if e.args[0] != NO_SUCH_TABLE:
                raise
            self._dedup = None
        if self.LOCK:
            self._todel_min_id = 0

    _connect = auto_reconnect(_tryConnect)

    def autoReconnect(self, f):
        assert not self.LOCK, "not a secondary connection"
        while True:
            try:
                return f()
            except DatabaseFailure as e:
                e.checkTransientFailure(self)

    def _commit(self):
        # XXX: Should we translate OperationalError into MysqlError ?
        self.conn.commit()
        self._active = 0

    @auto_reconnect
    def query(self, query):
        """Query data from a database."""
        assert self.lock._is_owned()
        assert type(query) is bytes
        if LOG_QUERIES:
            q = query.split(b'\n', 1)[0][:LOG_QUERIES]
            logging.debug('[%s] querying %s%s',
                          id(self), getPrintableQuery(q),
                          '' if q == query else '...')
        conn = self.conn
        conn.query(query)
        if query.startswith(b"SELECT "):
            return fetch_all(conn)
        r = query.split(None, 1)[0]
        if r in (b"INSERT", b"REPLACE", b"DELETE", b"UPDATE", b"SET"):
            self._active = 1
        else: # DDL (implicit commits)
            assert r in (b"ALTER", b"CREATE", b"DROP", b"TRUNCATE"), query
            assert self.LOCK, "not a primary connection"
            self._last_commit = time.time()
            self._active = self._deferred_commit = 0

    if six.PY3:
        def query_str(self, query):
            return self.query(query.encode())
    else:
        query_str = query

    def _getDevPath(self):
        # BBB: MySQL is moving to Performance Schema.
        return self.query(b"SELECT * FROM information_schema.global_variables"
                          b" WHERE variable_name='datadir'")[0][1]

    def erase(self):
        self.query_str("DROP TABLE IF EXISTS"
            " config, pt, pack, trans, obj, data, bigdata, ttrans, tobj, todel")

    def nonempty(self, table):
        try:
            return bool(self.query_str("SELECT 1 FROM %s LIMIT 1" % table))
        except ProgrammingError as e:
            if e.args[0] != NO_SUCH_TABLE:
                raise

    def _alterTable(self, schema_dict, table, select="*"):
        q = self.query_str
        new = 'new_' + table
        if self.nonempty(table) is None:
            if self.nonempty(new) is None:
                return
        else:
            q("DROP TABLE IF EXISTS " + new)
            q(schema_dict.pop(table) % new
              + " SELECT %s FROM %s" % (select, table))
            q("DROP TABLE " + table)
        q("ALTER TABLE %s RENAME TO %s" % (new, table))

    def _migrate1(self, _):
        self._checkNoUnfinishedTransactions()
        self.query(b"DROP TABLE IF EXISTS ttrans")

    def _migrate2(self, schema_dict):
        self._alterTable(schema_dict, 'obj')

    def _migrate3(self, schema_dict):
        x = 'pt'
        self._alterTable({x: schema_dict[x].replace('pack', '-- pack')}, x,
            "rid AS `partition`, nid,"
            " CASE state"
            " WHEN 0 THEN -1"  # UP_TO_DATE
            " WHEN 2 THEN -2"  # FEEDING
            " ELSE 1-state"
            " END AS tid")

    def _migrate4(self, schema_dict):
        self._setConfiguration('partitions', None)
        self._alterTable(schema_dict, 'pt', "*,"
            " IF(nid=%s, 0, NULL) AS pack" % self.getUUID())

    def _setup(self, dedup=False):
        self._config.clear()
        q = self.query_str
        p = engine = self._engine
        schema_dict = OrderedDict()

        # The table "config" stores configuration
        # parameters which affect the persistent data.
        schema_dict['config'] = """CREATE TABLE %s (
                  name VARBINARY(255) NOT NULL PRIMARY KEY,
                  value VARBINARY(255) NULL
              ) ENGINE=""" + engine

        # The table "pt" stores a partition table.
        schema_dict['pt'] = """CREATE TABLE %s (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 nid INT NOT NULL,
                 tid BIGINT NOT NULL,
                 pack BIGINT UNSIGNED,
                 PRIMARY KEY (`partition`, nid)
             ) ENGINE=""" + engine

        schema_dict['pack'] = """CREATE TABLE %s (
                  tid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                  approved BOOLEAN, -- NULL if not signed
                  partial BOOLEAN NOT NULL,
                  oids MEDIUMBLOB, -- same format as trans.oids
                  pack_tid BIGINT UNSIGNED
             ) ENGINE=""" + engine

        if engine == "RocksDB":
            cf = lambda name, rev=False: " COMMENT '%scf_neo_%s'" % (
                'rev:' if rev else '', name)
        else:
            cf = lambda *_: ''

        # The table "trans" stores information on committed transactions.
        schema_dict['trans'] =  """CREATE TABLE %s (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid BIGINT UNSIGNED NOT NULL,
                 PRIMARY KEY (`partition`, tid){}
             ) ENGINE={}""".format(cf('append_meta'), p)

        # The table "obj" stores committed object metadata.
        schema_dict['obj'] = """CREATE TABLE %s (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 oid BIGINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 data_id BIGINT UNSIGNED NULL,
                 value_tid BIGINT UNSIGNED NULL,
                 PRIMARY KEY (`partition`, oid, tid){},
                 KEY tid (`partition`, tid, oid){},
                 KEY (data_id){}
             ) ENGINE={}""".format(cf('obj_pk', True),
                 cf('append_meta'), cf('append_meta'), p)

        # The table "data" stores object data.
        # We'd like to have partial index on 'hash' column (e.g. hash(4))
        # but 'UNIQUE' constraint would not work as expected.
        schema_dict['data'] = """CREATE TABLE %s (
                 id BIGINT UNSIGNED NOT NULL,
                 hash BINARY(20) NOT NULL,
                 compression TINYINT UNSIGNED NULL,
                 value MEDIUMBLOB NOT NULL,
                 PRIMARY KEY (id){}{}
             ) ENGINE={}""".format(cf('append'), """,
                 UNIQUE (hash, compression)""" + cf('no_comp') if dedup else "",
                 engine)

        schema_dict['bigdata'] = """CREATE TABLE %s (
                 id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                 value MEDIUMBLOB NOT NULL,
                 PRIMARY KEY (id){}
             ) ENGINE={}""".format(cf('append'), p)

        # The table "ttrans" stores information on uncommitted transactions.
        schema_dict['ttrans'] = """CREATE TABLE %s (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED,
                 packed BOOLEAN NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid BIGINT UNSIGNED NOT NULL,
                 PRIMARY KEY (ttid){}
             ) ENGINE={}""".format(cf('no_comp'), p)

        # The table "tobj" stores uncommitted object metadata.
        schema_dict['tobj'] = """CREATE TABLE %s (
                 `partition` SMALLINT UNSIGNED NOT NULL,
                 oid BIGINT UNSIGNED NOT NULL,
                 tid BIGINT UNSIGNED NOT NULL,
                 data_id BIGINT UNSIGNED NULL,
                 value_tid BIGINT UNSIGNED NULL,
                 PRIMARY KEY (tid, oid){}
             ) ENGINE={}""".format(cf('no_comp'), p)

        # The table "todel" is used for deferred deletion of data rows.
        schema_dict['todel'] = """CREATE TABLE %s (
                 data_id BIGINT UNSIGNED NOT NULL,
                 PRIMARY KEY (data_id){}
             ) ENGINE={}""".format(cf('no_comp'), p)

        if self.nonempty('config') is None:
            q(schema_dict.pop('config') % 'config')
            self._setConfiguration('version', self.VERSION)
        else:
            self.migrate(schema_dict)

        for table, schema in six.iteritems(schema_dict):
            q(schema % ('IF NOT EXISTS ' + table))

        if self._dedup is None:
            self._dedup = dedup

        self._uncommitted_data.update(q("SELECT data_id, count(*)"
            " FROM tobj WHERE data_id IS NOT NULL GROUP BY data_id"))

    def getConfiguration(self, key):
        try:
            return self._config[key]
        except KeyError:
            assert type(key) is str
            try:
                r = bytes2str(self.query(b"SELECT value FROM config WHERE name = "
                                         + string_literal(key))[0][0])
            except IndexError:
                r = None
            self._config[key] = r
            return r

    def _setConfiguration(self, key, value):
        q = self.query
        self._config[key] = value
        assert type(key) is str
        k = string_literal(key)
        if value is None:
            q(b"DELETE FROM config WHERE name = " + k)
            return
        value = str(value)
        sql = b"REPLACE INTO config VALUES (%s, %s)" % (
            k, string_literal(value))
        try:
            q(sql)
        except DataError as e:
            if e.args[0] != DATA_TOO_LONG or len(value) < 256 or key != "zodb":
                raise
            q(b"ALTER TABLE config MODIFY value VARBINARY(%u) NULL"
                % len(value))
            q(sql)

    def _getMaxPartition(self):
        return self.query(b"SELECT MAX(`partition`) FROM pt")[0][0]

    def _getPartitionTable(self):
        return self.query(b"SELECT * FROM pt")

    def _getFirstTID(self, partition):
        (tid,), = self.query(
            b"SELECT MIN(tid) as t FROM trans FORCE INDEX (PRIMARY)"
            b" WHERE `partition`=%u" % partition)
        return util.u64(MAX_TID) if tid is None else tid

    def _getLastTID(self, partition, max_tid=None):
        # XXX: How to avoid Decimal when using COALESCE?
        sql = (b"SELECT MAX(tid) as t FROM trans"
               b" FORCE INDEX (PRIMARY) WHERE `partition`=%u") % partition
        if max_tid:
            sql += b" AND tid<=%u" % max_tid
        (tid,), = self.query(sql)
        return -1 if tid is None else tid

    def _getLastIDs(self, partition):
        # XXX: How to avoid Decimal when using COALESCE?
        (oid,), (tid,) = self.query(
            b"SELECT MAX(oid) FROM obj FORCE INDEX (PRIMARY)"
            b" WHERE `partition`=%u UNION ALL "
            b"SELECT MAX(tid) FROM obj FORCE INDEX (tid)"
            b" WHERE `partition`=%u" % (partition, partition))
        return -1 if tid is None else tid, -1 if oid is None else oid

    def _getPackOrders(self, min_completed):
        return self.query_str(
            "SELECT * FROM pack WHERE tid >= %s AND tid %% %s IN (%s)"
            " ORDER BY tid"
            % (min_completed, self.np, ','.join(map(str, self._readable_set))))

    def getPackedIDs(self, up_to_date=False):
        return {offset: util.p64(pack) for offset, pack in self.query(
            b"SELECT `partition`, pack FROM pt WHERE pack IS NOT NULL"
            + (b" AND tid=-%u" % CellStates.UP_TO_DATE if up_to_date else b""))}

    def _getPartitionPacked(self, partition):
        (pack_id,), = self.query(
            b"SELECT pack FROM pt WHERE `partition`=%u AND nid=%u"
            % (partition, self.getUUID()))
        assert pack_id is not None # PY3: the assertion will be useless because
                                   #      the caller always compares the value
        return pack_id

    def _setPartitionPacked(self, partition, pack_id):
        assert pack_id is not None
        self.query(b"UPDATE pt SET pack=%u WHERE `partition`=%u AND nid=%u"
                   % (pack_id, partition, self.getUUID()))

    def updateCompletedPackByReplication(self, partition, pack_id):
        pack_id = util.u64(pack_id)
        if __debug__:
            (i,), = self.query(
                b"SELECT pack FROM pt WHERE `partition`=%u AND nid=%u"
                % (partition, self.getUUID()))
            assert i is not None, i
        self.query(
            b"UPDATE pt SET pack=%u WHERE `partition`=%u AND nid=%u AND pack>%u"
            % (pack_id, partition, self.getUUID(), pack_id))

    def _getDataLastId(self, partition):
        return self.query(b"SELECT MAX(id) FROM data WHERE %u <= id AND id < %u"
            % (partition << 48, (partition + 1) << 48))[0][0]

    def _getUnfinishedTIDDict(self):
        q = self.query
        return q(b"SELECT ttid, tid FROM ttrans"), (ttid
            for ttid, in q(b"SELECT DISTINCT tid FROM tobj"))

    def getFinalTID(self, ttid):
        ttid = util.u64(ttid)
        # MariaDB is smart enough to realize that 'ttid' is constant.
        r = self.query(b"SELECT tid FROM trans"
            b" WHERE `partition`=%u AND tid>=ttid AND ttid=%u LIMIT 1"
            % (self._getReadablePartition(ttid), ttid))
        if r:
            return util.p64(r[0][0])

    def getLastObjectTID(self, oid):
        oid = util.u64(oid)
        r = self.query(b"SELECT tid FROM obj FORCE INDEX(PRIMARY)"
                       b" WHERE `partition`=%u AND oid=%u"
                       b" ORDER BY tid DESC LIMIT 1"
                       % (self._getReadablePartition(oid), oid))
        return util.p64(r[0][0]) if r else None

    def _getNextTID(self, *args): # partition, oid, tid
        r = self.query(b"SELECT tid FROM obj"
                       b" FORCE INDEX(PRIMARY)"
                       b" WHERE `partition`=%u AND oid=%u AND tid>%u"
                       b" ORDER BY tid LIMIT 1" % args)
        return r[0][0] if r else None

    def _getObject(self, oid, tid=None, before_tid=None):
        q = self.query
        partition = self._getReadablePartition(oid)
        sql = (b'SELECT tid, compression, data.hash, value, value_tid'
               b' FROM obj FORCE INDEX(PRIMARY)'
               b' LEFT JOIN data ON (obj.data_id = data.id)'
               b' WHERE `partition` = %u AND oid = %u') % (partition, oid)
        if before_tid is not None:
            sql += b' AND tid < %u ORDER BY tid DESC LIMIT 1' % before_tid
        elif tid is not None:
            sql += b' AND tid = %u' % tid
        else:
            # XXX I want to express "HAVING tid = MAX(tid)", but
            # MySQL does not use an index for a HAVING clause!
            sql += b' ORDER BY tid DESC LIMIT 1'
        r = q(sql)
        try:
            serial, compression, checksum, data, value_serial = r[0]
        except IndexError:
            return None
        if compression and compression & 0x80:
            compression &= 0x7f
            data = b''.join(self._bigData(data))
        return (serial, self._getNextTID(partition, oid, serial),
                compression, checksum, data, value_serial)

    def _changePartitionTable(self, cell_list, reset=False):
        q = self.query_str
        delete = set(q("SELECT `partition`, nid FROM pt")) if reset else set()
        for offset, nid, tid, pack in cell_list:
            key = offset, nid
            if tid is None:
                delete.add(key)
            else:
                delete.discard(key)
                q("INSERT INTO pt VALUES (%d, %d, %d, %s)"
                  " ON DUPLICATE KEY UPDATE tid=%d"
                  % (offset, nid, tid, 'NULL' if pack is None else pack, tid))
        if delete:
            q("DELETE FROM pt WHERE " + " OR ".join(
                map("`partition`=%s AND nid=%s".__mod__, delete)))

    def _dropPartition(self, offset, count):
        q = self.query
        where = b" WHERE `partition`=%u ORDER BY tid, oid LIMIT %u" % (
            offset, count)
        logging.debug("drop: select(%s)", count)
        x = q(b"SELECT DISTINCT data_id FROM obj FORCE INDEX(tid)" + where)
        if x:
            logging.debug("drop: obj")
            q(b"DELETE FROM obj" + where)
            return [x for x, in x]
        logging.debug("drop: trans")
        q(b"DELETE trans, pack FROM trans LEFT JOIN pack USING(tid)"
          b" WHERE `partition`=%u" % offset)
        (x,), = q(b'SELECT ROW_COUNT()')
        return x

    def _getUnfinishedDataIdList(self):
        return [x for x, in self.query(
            b"SELECT data_id FROM tobj WHERE data_id IS NOT NULL")]

    def dropPartitionsTemporary(self, offset_list=None):
        where = b"" if offset_list is None else b" WHERE `partition` IN (%s)" \
            % str2bytes(','.join(map(str, offset_list)))
        q = self.query
        q(b"DELETE FROM tobj" + where)
        q(b"DELETE ttrans, pack FROM ttrans LEFT JOIN pack USING(tid)" + where)

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        u64 = util.u64
        tid = u64(tid)
        if temporary:
            obj_table = b'tobj'
            trans_table = b'ttrans'
        else:
            obj_table = b'obj'
            trans_table = b'trans'
        q = self.query
        sql = [b"REPLACE INTO %s VALUES " % obj_table]
        values_max = self._max_allowed_packet - len(sql[0])
        values_size = 0
        for oid, data_id, value_serial in object_list:
            oid = u64(oid)
            partition = self._getPartition(oid)
            value = str2bytes("(%s,%s,%s,%s,%s)," % (
                partition, oid, tid,
                'NULL' if data_id is None else data_id,
                u64(value_serial) if value_serial else 'NULL'))
            values_size += len(value)
            # actually: max_values < values_size + EXTRA - len(final comma)
            # (test_max_allowed_packet checks that EXTRA == 2)
            if values_max <= values_size:
                sql[-1] = sql[-1][:-1] # remove final comma
                q(b''.join(sql))
                del sql[1:]
                values_size = len(value)
            sql.append(value)
        if values_size:
            sql[-1] = value[:-1] # remove final comma
            q(b''.join(sql))
        if transaction:
            oid_list, user, desc, ext, packed, ttid = transaction
            partition = self._getPartition(tid)
            e = string_literal
            assert packed in (0, 1)
            q(b"REPLACE INTO %s VALUES (%u,%s,%u,%s,%s,%s,%s,%u)" % (
                trans_table, partition,
                b'NULL' if temporary else b'%u' % tid,
                packed,
                e(b''.join(oid_list)), e(user), e(desc), e(ext), u64(ttid)))

    _structLL = struct.Struct(">LL")
    _unpackLL = _structLL.unpack

    def getOrphanList(self):
        return [x for x, in self.query_str(
            "SELECT id FROM data"
            " LEFT JOIN obj ON (id=obj.data_id)"
            " LEFT JOIN todel ON (id=todel.data_id)"
            " WHERE obj.data_id IS NULL"
            " AND todel.data_id IS NULL")]

    def _dataIdsToPrune(self, limit):
        min_id = self._todel_min_id
        data_id_list = [data_id for data_id, in self.query(
            b"SELECT data_id FROM todel WHERE data_id>=%u"
            b" ORDER BY data_id LIMIT %u" % (min_id, limit))]
        if data_id_list:
            self._todel_min_id = data_id_list[-1] + 1
        elif min_id:
            self._todel_min_id = 0
            self.query(b"TRUNCATE TABLE todel")
        return data_id_list

    def _pruneData(self, data_id_list):
        data_id_list = set(data_id_list).difference(self._uncommitted_data)
        if data_id_list:
            # Split the query to avoid exceeding max_allowed_packet.
            # Each id is 20 chars maximum.
            data_id_list = splitList(sorted(data_id_list), 1000000)
            q = self.query_str
            if self.LOCK is None:
                for data_id_list in data_id_list:
                    q("REPLACE INTO todel VALUES (%s)"
                      % "),(".join(map(str, data_id_list)))
                return
            id_list = []
            bigid_list = []
            for data_id_list in data_id_list:
                for id, value in q(
                        "SELECT id, IF(compression < 128, NULL, value)"
                        " FROM data LEFT JOIN obj ON (id = data_id)"
                        " WHERE id IN (%s) AND data_id IS NULL"
                        % ",".join(map(str, data_id_list))):
                    id_list.append(id)
                    if value:
                        bigdata_id, length = self._unpackLL(value)
                        bigid_list += range(
                            bigdata_id,
                            bigdata_id + (length + 0x7fffff >> 23))
            if id_list:
                def delete(table, id_list):
                    for id_list in splitList(id_list, 1000000):
                        q("DELETE FROM %s WHERE id IN (%s)"
                          % (table, ",".join(map(str, id_list))))
                delete('data', id_list)
                if bigid_list:
                    bigid_list.sort()
                    delete('bigdata', bigid_list)
                return len(id_list)
        return 0

    def _bigData(self, value):
        bigdata_id, length = self._unpackLL(value)
        q = self.query
        return (q(b"SELECT value FROM bigdata WHERE id=%u" % i)[0][0]
            for i in range(bigdata_id, bigdata_id + (length + 0x7fffff >> 23)))

    def storeData(self, checksum, oid, data, compression, data_tid,
            _pack=_structLL.pack):
        oid = util.u64(oid)
        p = self._getPartition(oid)
        if data_tid:
            for r, in self.query(b"SELECT data_id FROM obj"
                    b" WHERE `partition`=%u AND oid=%u AND tid=%u"
                    % (p, oid, util.u64(data_tid))):
                return r
            if p in self._readable_set: # and not checksum:
                raise UndoPackError
        if not checksum:
            return # delete
        e = string_literal
        checksum = e(checksum)
        if 0x1000000 <= len(data): # 16M (MEDIUMBLOB limit)
            compression |= 0x80
            q = self.query
            if self._dedup:
                for r, d in q(b"SELECT id, value FROM data"
                              b" WHERE hash=%s AND compression=%u"
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
            length = len(data)
            for j in range(0, length, 0x800000): # 8M
                q(b"INSERT INTO bigdata VALUES (%s, %s)"
                  % (b'%u' % i if j else b'NULL', e(data[j:j+0x800000])))
                if not j:
                    i = bigdata_id = self.conn.insert_id()
                i += 1
            data = _pack(bigdata_id, length)
        r = self._data_last_ids[p]
        try:
            self.query(b"INSERT INTO data VALUES (%u, %s, %u, %s)" %
                       (r, checksum, compression,  e(data)))
        except IntegrityError as e:
            if e.args[0] == DUP_ENTRY:
                (r, d), = self.query(b"SELECT id, value FROM data"
                                     b" WHERE hash=%s AND compression=%u"
                                     % (checksum, compression))
                if d == data:
                    return r
            raise
        self._data_last_ids[p] = r + 1
        return r

    def loadData(self, data_id):
        compression, hash, value = self.query(
            b"SELECT compression, hash, value FROM data where id=%u"
            % data_id)[0]
        if compression and compression & 0x80:
            compression &= 0x7f
            data = b''.join(self._bigData(data))
        return compression, hash, value

    del _structLL

    def storePackOrder(self, tid, approved, partial, oid_list, pack_tid):
        u64 = util.u64
        self.query(b"INSERT INTO pack VALUES (%u,%s,%u,%s,%u)" % (
            u64(tid),
            b'NULL' if approved is None else b'%u' % approved,
            partial,
            b'NULL' if oid_list is None else string_literal(b''.join(oid_list)),
            u64(pack_tid)))

    def _signPackOrders(self, approved, rejected):
        def isTID(x):
            return "tid IN (%s)" % ','.join(map(str, x)) if x else 0
        approved = isTID(approved)
        where = " WHERE %s OR %s" % (approved, isTID(rejected))
        q = self.query_str
        changed = [tid for tid, in q("SELECT tid FROM pack" + where)]
        if changed:
            q("UPDATE pack SET approved = %s%s" % (approved, where))
        return changed

    def lockTransaction(self, tid, ttid, pack):
        u64 = util.u64
        self.query(b"UPDATE ttrans SET tid=%u WHERE ttid=%u LIMIT 1"
                   % (u64(tid), u64(ttid)))
        if pack:
            self.query(b"UPDATE pack SET approved=1 WHERE tid=%u" % u64(ttid))
        self.commit()

    def unlockTransaction(self, tid, ttid, trans, obj, pack):
        q = self.query
        u64 = util.u64
        tid = u64(tid)
        if trans:
            q(b"INSERT INTO trans SELECT * FROM ttrans WHERE tid=%u" % tid)
            if pack:
                q(b"UPDATE pack SET tid=%u WHERE tid=%u" % (tid, u64(ttid)))
            q(b"DELETE FROM ttrans WHERE tid=%u" % tid)
            if not obj:
                return
        sql = b" FROM tobj WHERE tid=%u" % u64(ttid)
        data_id_list = [x for x, in q(
            b"SELECT data_id%s AND data_id IS NOT NULL" % sql)]
        q(b"INSERT INTO obj SELECT `partition`, oid, %u, data_id, value_tid %s"
          % (tid, sql))
        q(b"DELETE" + sql)
        self.releaseData(data_id_list)

    def abortTransaction(self, ttid):
        ttid = util.u64(ttid)
        q = self.query
        q(b"DELETE FROM tobj WHERE tid=%u" % ttid)
        q(b"DELETE FROM ttrans WHERE ttid=%u" % ttid)

    def deleteTransaction(self, tid):
        tid = util.u64(tid)
        self.query(b"DELETE trans, pack"
            b" FROM trans LEFT JOIN pack USING(tid)"
            b" WHERE `partition`=%u AND tid=%u" %
            (self._getPartition(tid), tid))

    def deleteObject(self, oid, serial=None):
        u64 = util.u64
        oid = u64(oid)
        sql = b" FROM obj WHERE `partition`=%u AND oid=%u" \
            % (self._getPartition(oid), oid)
        if serial:
            sql += b' AND tid=%u' % u64(serial)
        q = self.query
        data_id_list = [x for x, in q(
            b"SELECT DISTINCT data_id%s AND data_id IS NOT NULL" % sql)]
        q(b"DELETE" + sql)
        self._pruneData(data_id_list)

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        sql = b" WHERE `partition`=%u" % partition
        if min_tid is not None:
            sql += b" AND %u < tid" % min_tid
        if max_tid is not None:
            sql += b" AND tid <= %u" % max_tid
        q = self.query
        q(b"DELETE trans, pack FROM trans LEFT JOIN pack USING(tid)" + sql)
        sql = b" FROM obj" + sql
        data_id_list = [x for x, in q(
            b"SELECT DISTINCT data_id%s AND data_id IS NOT NULL" % sql)]
        q(b"DELETE" + sql)
        self._pruneData(data_id_list)

    def getTransaction(self, tid, all = False):
        tid = util.u64(tid)
        q = self.query
        r = q(b"SELECT trans.oids, user, description, ext, packed, ttid,"
                b" approved, partial, pack.oids, pack_tid"
              b" FROM trans LEFT JOIN pack USING (tid)"
              b" WHERE `partition` = %u AND tid = %u"
              % (self._getReadablePartition(tid), tid))
        if not r:
            if not all:
                return
            r = q(b"SELECT ttrans.oids, user, description, ext, packed, ttid,"
                    b" approved, partial, pack.oids, pack_tid"
                  b" FROM ttrans LEFT JOIN pack USING (tid)"
                  b" WHERE tid = %u" % tid)
            if not r:
                return
        oids, user, desc, ext, packed, ttid, \
            approved, pack_partial, pack_oids, pack_tid = r[0]
        return (
            splitOIDField(tid, oids),
            user, desc, ext,
            bool(packed), util.p64(ttid),
            None if pack_partial is None else (
                None if approved is None else bool(approved),
                bool(pack_partial),
                None if pack_oids is None else splitOIDField(tid, pack_oids),
                util.p64(pack_tid)))

    def _getObjectHistoryForUndo(self, oid, undo_tid):
        q = self.query
        args = self._getReadablePartition(oid), oid, undo_tid
        undo = iter(q(b"SELECT tid FROM obj"
                      b" WHERE `partition`=%u AND oid=%u AND tid<=%u"
                      b" ORDER BY tid DESC LIMIT 2" % args))
        if next(undo, (None,))[0] == undo_tid:
            return next(undo, (None,))[0], q(
                b"SELECT tid, value_tid FROM obj"
                b" WHERE `partition`=%u AND oid=%u AND tid>%u"
                b" ORDER BY tid" % args)

    def getObjectHistoryWithLength(self, oid, offset, length):
        # FIXME: This method doesn't take client's current transaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        oid = util.u64(oid)
        p64 = util.p64
        r = self.query(
            b"SELECT tid, IF(compression < 128, LENGTH(value),"
                b" CAST(CONV(HEX(SUBSTR(value, 5, 4)), 16, 10) AS INT))"
            b" FROM obj FORCE INDEX(PRIMARY)"
                b" LEFT JOIN data ON (obj.data_id = data.id)"
            b" WHERE `partition` = %u AND oid = %u"
            b" ORDER BY tid DESC LIMIT %u, %u" %
            (self._getReadablePartition(oid), oid, offset, length))
        if r:
            return [(p64(tid), length or 0) for tid, length in r]

    def _fetchObject(self, oid, tid):
        r = self.query(
            b'SELECT tid, compression, data.hash, value, value_tid'
            b' FROM obj FORCE INDEX(PRIMARY)'
            b' LEFT JOIN data ON (obj.data_id = data.id)'
            b' WHERE `partition` = %u AND oid = %u AND tid = %u'
            % (self._getReadablePartition(oid), oid, tid))
        if r:
            r = r[0]
            compression = r[1]
            if compression and compression & 0x80:
                return (r[0], compression & 0x7f, r[2],
                    b''.join(self._bigData(r[3])), r[4])
            return r

    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        r = self.query(b'SELECT tid, oid FROM obj FORCE INDEX(tid)'
                       b' WHERE `partition` = %d AND tid <= %d'
                       b' AND (tid = %u AND %u <= oid OR %u < tid)'
                       b' ORDER BY tid ASC, oid ASC%s' % (
            partition, u64(max_tid), min_tid, u64(min_oid), min_tid,
            b'' if length is None else b' LIMIT %u' % length))
        return [(p64(serial), p64(oid)) for serial, oid in r]

    def _getTIDList(self, offset, length, partition_list):
        return (t[0] for t in self.query_str(
            "SELECT tid FROM trans WHERE `partition` in (%s)"
            " ORDER BY tid DESC LIMIT %d,%d"
            % (','.join(map(str, partition_list)), offset, length)))

    def oidsFrom(self, partition, length, min_oid, tid):
        if partition not in self._readable_set:
            raise NonReadableCell
        p64 = util.p64
        u64 = util.u64
        r = self.query(b"SELECT oid, data_id"
            b" FROM obj FORCE INDEX(PRIMARY) JOIN ("
                b"SELECT `partition`, oid, MAX(tid) AS tid"
                b" FROM obj FORCE INDEX(PRIMARY)"
                b" WHERE `partition`=%u AND oid>=%u AND tid<=%u"
                b" GROUP BY oid ORDER BY oid LIMIT %u"
                b") AS t USING (`partition`, oid, tid)"
            % (partition, u64(min_oid), u64(tid), length))
        return None if len(r) < length else p64(r[-1][0] + self.np), \
            [p64(oid) for oid, data_id in r if data_id is not None]

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        u64 = util.u64
        p64 = util.p64
        r = self.query(b"SELECT tid FROM trans"
                       b" WHERE `partition` = %u AND tid >= %u AND tid <= %u"
                       b" ORDER BY tid ASC%s" % (
            partition, u64(min_tid), u64(max_tid),
            b'' if length is None else b' LIMIT %u' % length))
        return [p64(t[0]) for t in r]

    def _pack(self, offset, oid, tid, limit=None):
        q = self.query
        data_id_set = set()
        sql = (b"SELECT obj.oid AS oid,"
                b" IF(data_id IS NULL OR n>1, tid + (data_id IS NULL), NULL)"
            b" FROM (SELECT COUNT(*) AS n, oid, MAX(tid) AS max_tid"
                b" FROM obj FORCE INDEX(PRIMARY)"
                b" WHERE `partition`=%u AND oid%s AND tid<=%u"
                b" GROUP BY oid%s) AS t"
            b" JOIN obj ON `partition`=%u AND t.oid=obj.oid AND tid=max_tid"
            b" ORDER BY oid") % (
            offset,
            b">=%u" % oid if limit else
            b" IN (%s)" % str2bytes(','.join(map(str, oid))),
            tid,
            b" ORDER BY oid LIMIT %u" % limit if limit else b'',
            offset)
        oid = None
        for oid, tid in q(sql):
            if tid is not None:
                sql = b" FROM obj WHERE `partition`=%u AND oid=%u AND tid<%u" \
                    % (offset, oid, tid)
                data_id_set.update(*zip(*q(b"SELECT DISTINCT data_id" + sql)))
                q(b"DELETE" + sql)
        data_id_set.discard(None)
        self._pruneData(data_id_set)
        return limit and oid, len(data_id_set)

    def checkTIDRange(self, partition, length, min_tid, max_tid):
        u64 = util.u64
        count, tid_checksum, max_tid = self.query(
            b'SELECT COUNT(*), SHA1(GROUP_CONCAT(tid SEPARATOR ",")), MAX(tid)'
            b' FROM (SELECT tid FROM trans'
            b' WHERE `partition` = %u AND tid >= %u AND tid <= %u'
            b' ORDER BY tid ASC %s) AS t' % (
                partition, u64(min_tid), u64(max_tid),
                b'' if length is None else b'LIMIT %u' % length))[0]
        if count:
            return count, a2b_hex(tid_checksum), util.p64(max_tid)
        return 0, ZERO_HASH, ZERO_TID

    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        u64 = util.u64
        min_tid = u64(min_tid)
        # We don't ask MySQL to compute everything (like in checkTIDRange)
        # because it's difficult to get the last serial _for the last oid_.
        # We would need a function (that could be named 'LAST') that returns the
        # last grouped value, instead of the greatest one.
        r = self.query(
            b"SELECT tid, oid"
            b" FROM obj FORCE INDEX(tid)"
            b" WHERE `partition` = %u AND tid <= %u"
                b" AND (tid > %u OR tid = %u AND oid >= %u)"
            b" ORDER BY tid, oid %s" % (
                partition, u64(max_tid), min_tid, min_tid, u64(min_oid),
                b'' if length is None else b'LIMIT %u' % length))
        if r:
            p64 = util.p64
            return (len(r),
                    sha1(str2bytes(','.join(str(x[0]) for x in r))).digest(),
                    p64(r[-1][0]),
                    sha1(str2bytes(','.join(str(x[1]) for x in r))).digest(),
                    p64(r[-1][1]))
        return 0, ZERO_HASH, ZERO_TID, ZERO_HASH, ZERO_OID

    def _cmdline(self):
        for x in ('u', self.user), ('p', self.passwd), ('S', self.socket):
            if x[1]:
                yield '-%s%s' % x
        yield self.db

    def dump(self):
        import subprocess
        cmd = ['mysqldump', '--compact', '--hex-blob']
        cmd += self._cmdline()
        return bytes2str(subprocess.check_output(cmd))

    def _restore(self, sql):
        import subprocess
        cmd = ['mysql']
        cmd += self._cmdline()
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        p.communicate(str2bytes(sql))
        retcode = p.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd)
