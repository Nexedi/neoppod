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
from neo.protocol import CellStates, ZERO_OID, ZERO_TID
from neo import util

LOG_QUERIES = False

class CreationUndone(Exception):
    pass

def splitOIDField(tid, oids):
    if (len(oids) % 8) != 0 or len(oids) == 0:
        raise DatabaseFailure('invalid oids length for tid %d: %d' % (tid,
            len(oids)))
    oid_list = []
    append = oid_list.append
    for i in xrange(0, len(oids), 8):
        append(oids[i:i+8])
    return oid_list

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
        if LOG_QUERIES:
            logging.debug('committing...')
        self.conn.commit()

    def _rollback(self):
        if LOG_QUERIES:
            logging.debug('aborting...')
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
                self._connect()
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
                 value VARBINARY(255) NULL
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
                 compression TINYINT UNSIGNED NULL,
                 checksum INT UNSIGNED NULL,
                 value LONGBLOB NULL,
                 value_serial BIGINT UNSIGNED NULL,
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
                 compression TINYINT UNSIGNED NULL,
                 checksum INT UNSIGNED NULL,
                 value LONGBLOB NULL,
                 value_serial BIGINT UNSIGNED NULL
             ) ENGINE = InnoDB""")

    def getConfiguration(self, key):
        q = self.query
        e = self.escape
        key = e(str(key))
        try:
            return q("SELECT value FROM config WHERE name = '%s'" % key)[0][0]
        except IndexError:
            raise KeyError, key

    def _setConfiguration(self, key, value):
        q = self.query
        e = self.escape
        key = e(str(key))
        if value is None:
            value = 'NULL'
        else:
            value = "'%s'" % (e(str(value)), )
        q("""REPLACE INTO config VALUES ('%s', %s)""" % (key, value))

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            result = int(self.getConfiguration('_pack_tid'))
        except KeyError:
            result = -1
        return result

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

    def _getObjectData(self, oid, value_serial, tid):
        if value_serial is None:
            raise CreationUndone
        if value_serial >= tid:
            raise ValueError, "Incorrect value reference found for " \
                "oid %d at tid %d: reference = %d" % (oid, value_serial, tid)
        r = self.query("""SELECT compression, checksum, value, """ \
            """value_serial FROM obj WHERE oid = %d AND serial = %d""" % (
            oid, value_serial))
        compression, checksum, value, next_value_serial = r[0]
        if value is None:
            logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            value_serial, compression, checksum, value = self._getObjectData(
                oid, next_value_serial, value_serial)
        return value_serial, compression, checksum, value

    def _getObject(self, oid, tid=None, before_tid=None):
        q = self.query
        if tid is not None:
            r = q("""SELECT serial, compression, checksum, value, value_serial
                        FROM obj
                        WHERE oid = %d AND serial = %d""" \
                    % (oid, tid))
            try:
                serial, compression, checksum, data, value_serial = r[0]
                next_serial = None
            except IndexError:
                return None
        elif before_tid is not None:
            r = q("""SELECT serial, compression, checksum, value, value_serial
                        FROM obj
                        WHERE oid = %d AND serial < %d
                        ORDER BY serial DESC LIMIT 1""" \
                    % (oid, before_tid))
            try:
                serial, compression, checksum, data, value_serial = r[0]
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
            r = q("""SELECT serial, compression, checksum, value, value_serial
                        FROM obj
                        WHERE oid = %d ORDER BY serial DESC LIMIT 1""" \
                    % oid)
            try:
                serial, compression, checksum, data, value_serial = r[0]
                next_serial = None
            except IndexError:
                return None

        return serial, next_serial, compression, checksum, data, value_serial

    def getObject(self, oid, tid=None, before_tid=None, resolve_data=True):
        # TODO: resolve_data must be unit-tested
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        if tid is not None:
            tid = u64(tid)
        if before_tid is not None:
            before_tid = u64(before_tid)
        result = self._getObject(oid, tid, before_tid)
        if result is None:
            # See if object exists at all
            result = self._getObject(oid)
            if result is not None:
                # Object exists
                result = False
        else:
            serial, next_serial, compression, checksum, data, data_serial = \
                result
            if data is None and resolve_data:
                try:
                    _, compression, checksum, data = self._getObjectData(oid,
                        data_serial, serial)
                except CreationUndone:
                    compression = 0
                    # XXX: this is the valid checksum for empty string
                    checksum = 1
                    data = ''
                data_serial = None
            if serial is not None:
                serial = p64(serial)
            if next_serial is not None:
                next_serial = p64(next_serial)
            if data_serial is not None:
                data_serial = p64(data_serial)
            result = serial, next_serial, compression, checksum, data, data_serial
        return result

    def doSetPartitionTable(self, ptid, cell_list, reset):
        q = self.query
        e = self.escape
        self.begin()
        try:
            if reset:
                q("""TRUNCATE pt""")
            for offset, uuid, state in cell_list:
                uuid = e(util.dump(uuid))
                # TODO: this logic should move out of database manager
                # add 'dropCells(cell_list)' to API and use one query
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

    def dropPartitions(self, num_partitions, offset_list):
        q = self.query
        e = self.escape
        offset_list = ', '.join((str(i) for i in offset_list))
        self.begin()
        try:
            q("""DELETE FROM obj WHERE MOD(oid, %d) IN (%s)""" %
                (num_partitions, offset_list))
            q("""DELETE FROM trans WHERE MOD(tid, %d) IN (%s)""" %
                (num_partitions, offset_list))
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
        u64 = util.u64
        tid = u64(tid)

        if temporary:
            obj_table = 'tobj'
            trans_table = 'ttrans'
        else:
            obj_table = 'obj'
            trans_table = 'trans'

        self.begin()
        try:
            for oid, compression, checksum, data, value_serial in object_list:
                oid = u64(oid)
                if data is None:
                    compression = checksum = data = 'NULL'
                else:
                    # TODO: unit-test this raise
                    if value_serial is not None:
                        raise ValueError, 'Either data or value_serial ' \
                            'must be None (oid %d, tid %d)' % (oid, tid)
                    compression = '%d' % (compression, )
                    checksum = '%d' % (checksum, )
                    data = "'%s'" % (e(data), )
                if value_serial is None:
                    value_serial = 'NULL'
                else:
                    value_serial = '%d' % (u64(value_serial), )
                q("""REPLACE INTO %s VALUES (%d, %d, %s, %s, %s, %s)""" \
                        % (obj_table, oid, tid, compression, checksum, data,
                            value_serial))
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

    def _getDataTIDFromData(self, oid, result):
        tid, next_serial, compression, checksum, data, value_serial = result
        if data is None:
            try:
                data_serial = self._getObjectData(oid, value_serial, tid)[0]
            except CreationUndone:
                data_serial = None
        else:
            data_serial = tid
        return tid, data_serial

    def _getDataTID(self, oid, tid=None, before_tid=None):
        """
        Return a 2-tuple:
        tid (int)
            tid corresponding to received parameters
        serial
            tid at which actual object data is located

        If 'tid is None', requested object and transaction could
        not be found.
        If 'serial is None', requested object exist but has no data (its creation
        has been undone).
        If 'tid == serial', it means that requested transaction
        contains object data.
        Otherwise, it's an undo transaction which did not involve conflict
        resolution.
        """
        result = self._getObject(oid, tid=tid, before_tid=before_tid)
        if result is None:
            result = (None, None)
        else:
            result = self._getDataTIDFromData(oid, result)
        return result

    def findUndoTID(self, oid, tid, undone_tid, transaction_object):
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        tid = u64(tid)
        undone_tid = u64(undone_tid)
        _getDataTID = self._getDataTID
        if transaction_object is not None:
            toid, tcompression, tchecksum, tdata, tvalue_serial = \
                transaction_object
            current_tid, current_data_tid = self._getDataTIDFromData(oid,
                (tid, None, tcompression, tchecksum, tdata,
                u64(tvalue_serial)))
        else:
            current_tid, current_data_tid = _getDataTID(oid, before_tid=tid)
        if current_tid is None:
            return (None, None, False)
        found_undone_tid, undone_data_tid = _getDataTID(oid, tid=undone_tid)
        assert found_undone_tid is not None, (oid, undone_tid)
        is_current = undone_data_tid in (current_data_tid, tid)
        # Load object data as it was before given transaction.
        # It can be None, in which case it means we are undoing object
        # creation.
        _, data_tid = _getDataTID(oid, before_tid=undone_tid)
        if data_tid is not None:
            data_tid = p64(data_tid)
        return p64(current_tid), data_tid, is_current

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

    def deleteObject(self, oid, serial=None):
        u64 = util.u64
        query_param_dict = {
            'oid': u64(oid),
        }
        query_fmt = 'DELETE FROM obj WHERE oid = %(oid)d'
        if serial is not None:
            query_param_dict['serial'] = u64(serial)
            query_fmt = query_fmt + ' AND serial = %(serial)d'
        self.begin()
        try:
            self.query(query_fmt % query_param_dict)
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
            oid_list = splitOIDField(tid, oids)
            return oid_list, user, desc, ext, bool(packed)
        return None

    def _getObjectLength(self, oid, value_serial):
        if value_serial is None:
            raise CreationUndone
        r = self.query("""SELECT LENGTH(value), value_serial FROM obj """ \
            """WHERE oid = %d AND serial = %d""" % (oid, value_serial))
        length, value_serial = r[0]
        if length is None:
            logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            length = self._getObjectLength(oid, value_serial)
        return length

    def getObjectHistory(self, oid, offset = 0, length = 1):
        # FIXME: This method doesn't take client's current ransaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        q = self.query
        oid = util.u64(oid)
        p64 = util.p64
        pack_tid = self._getPackTID()
        r = q("""SELECT serial, LENGTH(value), value_serial FROM obj
                    WHERE oid = %d AND serial >= %d
                    ORDER BY serial DESC LIMIT %d, %d""" \
                % (oid, pack_tid, offset, length))
        if r:
            result = []
            append = result.append
            for serial, length, value_serial in r:
                if length is None:
                    try:
                        length = self._getObjectLength(oid, value_serial)
                    except CreationUndone:
                        length = 0
                append((p64(serial), length))
            return result
        return None

    def getObjectHistoryFrom(self, min_oid, min_serial, max_serial, length,
            num_partitions, partition):
        q = self.query
        u64 = util.u64
        p64 = util.p64
        min_oid = u64(min_oid)
        min_serial = u64(min_serial)
        max_serial = u64(max_serial)
        r = q('SELECT oid, serial FROM obj '
                'WHERE ((oid = %(min_oid)d AND serial >= %(min_serial)d) OR '
                'oid > %(min_oid)d) AND '
                'MOD(oid, %(num_partitions)d) = %(partition)s AND '
                'serial <= %(max_serial)d '
                'ORDER BY oid ASC, serial ASC LIMIT %(length)d' % {
            'min_oid': min_oid,
            'min_serial': min_serial,
            'max_serial': max_serial,
            'length': length,
            'num_partitions': num_partitions,
            'partition': partition,
        })
        result = {}
        for oid, serial in r:
            try:
                serial_list = result[oid]
            except KeyError:
                serial_list = result[oid] = []
            serial_list.append(p64(serial))
        return dict((p64(x), y) for x, y in result.iteritems())

    def getTIDList(self, offset, length, num_partitions, partition_list):
        q = self.query
        r = q("""SELECT tid FROM trans WHERE MOD(tid, %d) in (%s)
                    ORDER BY tid DESC LIMIT %d,%d""" \
                % (num_partitions,
                   ','.join([str(p) for p in partition_list]),
                   offset, length))
        return [util.p64(t[0]) for t in r]

    def getReplicationTIDList(self, min_tid, max_tid, length, num_partitions,
            partition):
        q = self.query
        u64 = util.u64
        p64 = util.p64
        min_tid = u64(min_tid)
        max_tid = u64(max_tid)
        r = q("""SELECT tid FROM trans WHERE
                    MOD(tid, %(num_partitions)d) = %(partition)d
                    AND tid >= %(min_tid)d AND tid <= %(max_tid)d
                    ORDER BY tid ASC LIMIT %(length)d""" % {
            'num_partitions': num_partitions,
            'partition': partition,
            'min_tid': min_tid,
            'max_tid': max_tid,
            'length': length,
        })
        return [p64(t[0]) for t in r]

    def _updatePackFuture(self, oid, orig_serial, max_serial,
            updateObjectDataForPack):
        q = self.query
        p64 = util.p64
        # Before deleting this objects revision, see if there is any
        # transaction referencing its value at max_serial or above.
        # If there is, copy value to the first future transaction. Any further
        # reference is just updated to point to the new data location.
        value_serial = None
        for table in ('obj', 'tobj'):
            for (serial, ) in q('SELECT serial FROM %(table)s WHERE '
                    'oid = %(oid)d AND serial >= %(max_serial)d AND '
                    'value_serial = %(orig_serial)d ORDER BY serial ASC' % {
                        'table': table,
                        'oid': oid,
                        'orig_serial': orig_serial,
                        'max_serial': max_serial,
                    }):
                if value_serial is None:
                    # First found, copy data to it and mark its serial for
                    # future reference.
                    value_serial = serial
                    q('REPLACE INTO %(table)s (oid, serial, compression, '
                        'checksum, value, value_serial) SELECT oid, '
                        '%(serial)d, compression, checksum, value, NULL FROM '
                        'obj WHERE oid = %(oid)d AND serial = %(orig_serial)d' \
                        % {
                            'table': table,
                            'oid': oid,
                            'serial': serial,
                            'orig_serial': orig_serial,
                    })
                else:
                    q('REPLACE INTO %(table)s (oid, serial, value_serial) '
                        'VALUES (%(oid)d, %(serial)d, %(value_serial)d)' % {
                            'table': table,
                            'oid': oid,
                            'serial': serial,
                            'value_serial': value_serial,
                    })
        def getObjectData():
            assert value_serial is None
            return q('SELECT compression, checksum, value FROM obj WHERE '
                'oid = %(oid)d AND serial = %(orig_serial)d' % {
                    'oid': oid,
                    'orig_serial': orig_serial,
                })[0]
        if value_serial:
            value_serial = p64(value_serial)
        updateObjectDataForPack(p64(oid), p64(orig_serial), value_serial,
            getObjectData)

    def pack(self, tid, updateObjectDataForPack):
        # TODO: unit test (along with updatePackFuture)
        q = self.query
        tid = util.u64(tid)
        updatePackFuture = self._updatePackFuture
        self.begin()
        try:
            self._setPackTID(tid)
            for count, oid, max_serial in q('SELECT COUNT(*) - 1, oid, '
                    'MAX(serial) FROM obj WHERE serial <= %(tid)d '
                    'GROUP BY oid' % {'tid': tid}):
                if q('SELECT LENGTH(value) FROM obj WHERE oid = %(oid)d AND '
                        'serial = %(max_serial)d' % {
                            'oid': oid,
                            'max_serial': max_serial,
                        })[0][0] == 0:
                    count += 1
                    max_serial += 1
                if count:
                    # There are things to delete for this object
                    for (serial, ) in q('SELECT serial FROM obj WHERE '
                            'oid=%(oid)d AND serial < %(max_serial)d' % {
                                'oid': oid,
                                'max_serial': max_serial,
                            }):
                        updatePackFuture(oid, serial, max_serial,
                            updateObjectDataForPack)
                        q('DELETE FROM obj WHERE oid=%(oid)d AND '
                            'serial=%(serial)d' % {
                                'oid': oid,
                                'serial': serial
                        })
        except:
            self.rollback()
            raise
        self.commit()
  
    def checkTIDRange(self, min_tid, length, num_partitions, partition):
        # XXX: XOR is a lame checksum
        count, tid_checksum, max_tid = self.query('SELECT COUNT(*), '
            'BIT_XOR(tid), MAX(tid) FROM ('
              'SELECT tid FROM trans '
              'WHERE MOD(tid, %(num_partitions)d) = %(partition)s '
              'AND tid >= %(min_tid)d '
              'ORDER BY tid ASC LIMIT %(length)d'
            ') AS foo' % {
                'num_partitions': num_partitions,
                'partition': partition,
                'min_tid': util.u64(min_tid),
                'length': length,
        })[0]
        if count == 0:
            tid_checksum = 0
            max_tid = ZERO_TID
        else:
            max_tid = util.p64(max_tid)
        return count, tid_checksum, max_tid

    def checkSerialRange(self, min_oid, min_serial, length, num_partitions,
            partition):
        # XXX: XOR is a lame checksum
        u64 = util.u64
        p64 = util.p64
        r = self.query('SELECT oid, serial FROM obj WHERE '
            '(oid > %(min_oid)d OR '
            '(oid = %(min_oid)d AND serial >= %(min_serial)d)) '
            'AND MOD(oid, %(num_partitions)d) = %(partition)s '
            'ORDER BY oid ASC, serial ASC LIMIT %(length)d' % {
                'min_oid': u64(min_oid),
                'min_serial': u64(min_serial),
                'length': length,
                'num_partitions': num_partitions,
                'partition': partition,
        })
        count = len(r)
        oid_checksum = serial_checksum = 0
        if count == 0:
            max_oid = ZERO_OID
            max_serial = ZERO_TID
        else:
            for max_oid, max_serial in r:
                oid_checksum ^= max_oid
                serial_checksum ^= max_serial
            max_oid = p64(max_oid)
            max_serial = p64(max_serial)
        return count, oid_checksum, max_oid, serial_checksum, max_serial

