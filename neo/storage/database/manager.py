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

import os, errno, socket, struct, sys, threading
from collections import defaultdict
from contextlib import contextmanager
from copy import copy
from functools import wraps
from neo.lib import logging, util
from neo.lib.interfaces import abstract, requires
from neo.lib.protocol import CellStates, NonReadableCell, MAX_TID, ZERO_TID
from . import DatabaseFailure

READABLE = CellStates.UP_TO_DATE, CellStates.FEEDING

def lazymethod(func):
    def getter(self):
        cls = self.__class__
        name = func.__name__
        assert name not in cls.__dict__
        setattr(cls, name, func(self))
        return getattr(self, name)
    return property(getter, doc=func.__doc__)

def fallback(func):
    def warn(self):
        logging.info("Fallback to generic/slow implementation of %s."
            " It should be overridden by backend storage (%s).",
            func.__name__, self.__class__.__name__)
        return func
    return lazymethod(wraps(func)(warn))

def splitOIDField(tid, oids):
    if len(oids) % 8:
        raise DatabaseFailure('invalid oids length for tid %s: %s'
            % (tid, len(oids)))
    return [oids[i:i+8] for i in xrange(0, len(oids), 8)]

class CreationUndone(Exception):
    pass

class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    ENGINES = ()
    UNSAFE = False

    __lock = None
    LOCK = "neostorage"
    LOCKED = "error: database is locked"

    _deferred = 0
    _repairing = None

    def __init__(self, database, engine=None, wait=None):
        """
            Initialize the object.
        """
        if engine:
            if engine not in self.ENGINES:
                raise ValueError("Unsupported engine: %r not in %r"
                                 % (engine, self.ENGINES))
            self._engine = engine
        # XXX: Maybe the default should be to retry indefinitely.
        #      But for unit tests, we really want to never retry.
        self._wait = wait or 0
        self._parse(database)
        self._init_attrs = tuple(self.__dict__)
        self._connect()

    def __getstate__(self):
        state = {x: getattr(self, x) for x in self._init_attrs}
        assert state # otherwise, __setstate__ is not called
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # For the moment, no need to duplicate secondary connections.
        #self._init_attrs = tuple(self.__dict__)
        # Secondary connections don't lock.
        self.LOCK = None
        self._connect()

    @contextmanager
    def _duplicate(self):
        db = copy(self)
        try:
            yield db
        finally:
            db.close()

    def __getattr__(self, attr):
        if attr in ('_readable_set', '_getPartition', '_getReadablePartition'):
            self._updateReadable()
        return self.__getattribute__(attr)

    def _partitionTableChanged(self):
        try:
            del (self._readable_set,
                 self._getPartition,
                 self._getReadablePartition)
        except AttributeError:
            pass

    def __enter__(self):
        assert not self.LOCK, "not a secondary connection"
        # XXX: All config caching should be done in this class,
        #      rather than in backend classes.
        self._config.clear()
        self._partitionTableChanged()

    def __exit__(self, t, v, tb):
        if v is None:
            # Deferring commits make no sense for secondary connections.
            assert not self._deferred
            self._commit()

    @abstract
    def _parse(self, database):
        """Called during instantiation, to process database parameter."""

    @abstract
    def _connect(self):
        """Connect to the database"""

    def autoReconnect(self, f):
        """
        Placeholder for backends that may lose connection to the underlying
        database: although a primary connection is reestablished transparently
        when possible, secondary connections use transactions and they must
        restart from the beginning.
        For other backends, there's no expected transient failure so the
        default implementation is to execute the given task exactly once.
        """
        f()

    def lock(self, db_path):
        if self.LOCK:
            assert self.__lock is None, self.__lock
            # For platforms that don't support anonymous sockets,
            # we can either use zc.lockfile or an empty SQLite db
            # (with BEGIN EXCLUSIVE).
            try:
                stat = os.stat(db_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                return # in-memory or temporary database
            s = self.__lock = socket.socket(socket.AF_UNIX)
            try:
                s.bind('\0%s:%s:%s' % (self.LOCK, stat.st_dev, stat.st_ino))
            except socket.error as e:
                if e.errno != errno.EADDRINUSE:
                    raise
                sys.exit(self.LOCKED)

    def _getDevPath(self):
        """
        """

    @requires(_getDevPath)
    def getTopologyPath(self):
        # On Windows, st_dev only exists since Python 3.4
        return socket.gethostname(), str(os.stat(self._getDevPath()).st_dev)

    @abstract
    def erase(self):
        """"""

    def _setup(self, dedup=False):
        """To be overridden by the backend to set up a database

        It must recover self._uncommitted_data from temporary object table.
        _uncommitted_data is already instantiated and must be updated with
        refcounts to data of write-locked objects, except in case of undo,
        where the refcount is increased later, when the object is read-locked.
        Keys are data ids and values are number of references.
        """

    @requires(_setup)
    def setup(self, reset=False, dedup=False):
        """Set up a database, discarding existing data first if reset is True
        """
        if reset:
            self.erase()
        self._uncommitted_data = defaultdict(int)
        self._setup(dedup)

    @abstract
    def nonempty(self, table):
        """Check whether table is empty or return None if it does not exist"""

    def _checkNoUnfinishedTransactions(self, *hint):
        if self.nonempty('ttrans') or self.nonempty('tobj'):
            raise DatabaseFailure(
                "The database can not be upgraded because you have unfinished"
                " transactions. Use an older version of NEO to verify them.")

    def migrate(self, *args, **kw):
        version = int(self.getConfiguration("version") or 0)
        if self.VERSION < version:
            raise DatabaseFailure("The database can not be downgraded.")
        while version < self.VERSION:
            version += 1
            getattr(self, '_migrate%s' % version)(*args, **kw)
            self.setConfiguration("version", version)

    def doOperation(self, app):
        pass

    def _close(self):
        """Backend-specific code to close the database"""

    @requires(_close)
    def close(self):
        self._deferredCommit()
        self._close()
        if self.__lock:
            self.__lock.close()
            del self.__lock

    def _commit(self):
        """Backend-specific code to commit the pending changes"""

    @requires(_commit)
    def commit(self):
        logging.debug('committing...')
        self._commit()
        # Instead of cancelling a timeout that would be set to defer a commit,
        # we simply use to a boolean so that _deferredCommit() does nothing.
        # IOW, epoll may wait wake up for nothing but that should be rare,
        # because most immediate commits are usually quickly followed by
        # deferred commits.
        self._deferred = 0

    def deferCommit(self):
        self._deferred = 1
        return self._deferredCommit

    def _deferredCommit(self):
        if self._deferred:
            self.commit()

    @abstract
    def getConfiguration(self, key):
        """
            Return a configuration value, returns None if not found or not set
        """

    def setConfiguration(self, key, value):
        """
            Set a configuration value
        """
        self._setConfiguration(key, value)
        self.commit()

    @abstract
    def _setConfiguration(self, key, value):
        """"""

    def getUUID(self):
        """
            Load a NID from a database.
        """
        nid = self.getConfiguration('nid')
        if nid is not None:
            return int(nid)

    def setUUID(self, nid):
        """
            Store a NID into a database.
        """
        self.setConfiguration('nid', str(nid))

    def getNumPartitions(self):
        """
            Load the number of partitions from a database.
        """
        n = self.getConfiguration('partitions')
        if n is not None:
            return int(n)

    def setNumPartitions(self, num_partitions):
        """
            Store the number of partitions into a database.
        """
        self.setConfiguration('partitions', num_partitions)
        self._partitionTableChanged()

    def getNumReplicas(self):
        """
            Load the number of replicas from a database.
        """
        n = self.getConfiguration('replicas')
        if n is not None:
            return int(n)

    def setNumReplicas(self, num_replicas):
        """
            Store the number of replicas into a database.
        """
        self.setConfiguration('replicas', num_replicas)

    def getName(self):
        """
            Load a name from a database.
        """
        return self.getConfiguration('name')

    def setName(self, name):
        """
            Store a name into a database.
        """
        self.setConfiguration('name', name)

    def getPTID(self):
        """
            Load a Partition Table ID from a database.
        """
        ptid = self.getConfiguration('ptid')
        if ptid is not None:
            return int(ptid)

    def getBackupTID(self):
        return util.bin(self.getConfiguration('backup_tid'))

    def _setBackupTID(self, tid):
        tid = util.dump(tid)
        logging.debug('backup_tid = %s', tid)
        return self._setConfiguration('backup_tid', tid)

    def getTruncateTID(self):
        return util.bin(self.getConfiguration('truncate_tid'))

    def _setTruncateTID(self, tid):
        tid = util.dump(tid)
        logging.debug('truncate_tid = %s', tid)
        return self._setConfiguration('truncate_tid', tid)

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            return int(self.getConfiguration('_pack_tid'))
        except TypeError:
            return -1

    # XXX: Consider splitting getLastIDs/_getLastIDs because
    #      sometimes the last oid is not wanted.

    def _getLastTID(self, partition, max_tid=None):
        """Return tid of last transaction <= 'max_tid' in given 'partition'

        tids are in unpacked format.
        """

    @requires(_getLastTID)
    def getLastTID(self, max_tid=None):
        """Return tid of last transaction <= 'max_tid'

        tids are in unpacked format.
        """
        if self.getNumPartitions():
            return max(map(self._getLastTID, self._readable_set))

    def _getLastIDs(self, partition):
        """Return max(tid) & max(oid) for objects of given partition

        Results are in unpacked format
        """

    @requires(_getLastIDs)
    def getLastIDs(self):
        """Return max(tid) & max(oid) for readable data

        It is important to ignore unassigned partitions because there may
        remain data from cells that have been discarded, either due to
        --disable-drop-partitions option, or in the future when dropping
        partitions is done in background (as it is an expensive operation).
        """
        x = self._readable_set
        if x:
            tid, oid = zip(*map(self._getLastIDs, x))
            tid = max(self.getLastTID(None), max(tid))
            oid = max(oid)
            return (None if tid is None else util.p64(tid),
                    None if oid is None else util.p64(oid))
        return None, None

    def _getUnfinishedTIDDict(self):
        """"""

    @requires(_getUnfinishedTIDDict)
    def getUnfinishedTIDDict(self):
        trans, obj = self._getUnfinishedTIDDict()
        obj = dict.fromkeys(obj)
        obj.update(trans)
        p64 = util.p64
        return {p64(ttid): None if tid is None else p64(tid)
                for ttid, tid in obj.iteritems()}

    @fallback
    def getLastObjectTID(self, oid):
        """Return the latest tid of given oid or None if it does not exist"""
        r = self.getObject(oid)
        return r and r[0]

    @abstract
    def _getNextTID(self, partition, oid, tid):
        """
        partition (int)
            Must be the result of (oid % self.getPartition(oid))
        oid (int)
            Identifier of object to retrieve.
        tid (int)
            Exact serial to retrieve.

        If tid is the last revision of oid, None is returned.
        """

    def _getObject(self, oid, tid=None, before_tid=None):
        """
        oid (int)
            Identifier of object to retrieve.
        tid (int, None)
            Exact serial to retrieve.
        before_tid (int, None)
            Serial to retrieve is the highest existing one strictly below this
            value.

        Return value:
            None: oid doesn't exist at requested tid/before_tid (getObject
                  takes care of checking if the oid exists at other serial)
            6-tuple: Record content.
                - record serial (int)
                - serial or next record modifying object (int, None)
                - compression (boolean-ish, None)
                - checksum (binary string, None)
                - data (binary string, None)
                - data_serial (int, None)
        """

    @requires(_getObject)
    def getObject(self, oid, tid=None, before_tid=None):
        """
        oid (packed)
            Identifier of object to retrieve.
        tid (packed, None)
            Exact serial to retrieve.
        before_tid (packed, None)
            Serial to retrieve is the highest existing one strictly below this
            value.

        Return value:
            None: Given oid doesn't exist in database.
            False: No record found, but another one exists for given oid.
            6-tuple: Record content.
                - record serial (packed)
                - serial or next record modifying object (packed, None)
                - compression (boolean-ish, None)
                - checksum (binary string, None)
                - data (binary string, None)
                - data_serial (packed, None)
        """
        u64 = util.u64
        r = self._getObject(u64(oid), tid and u64(tid),
                            before_tid and u64(before_tid))
        try:
            serial, next_serial, compression, checksum, data, data_serial = r
        except TypeError:
            # See if object exists at all
            return (tid or before_tid) and self.getLastObjectTID(oid) and False
        return (util.p64(serial),
                None if next_serial is None else util.p64(next_serial),
                compression, checksum, data,
                None if data_serial is None else util.p64(data_serial))

    @fallback
    def _fetchObject(self, oid, tid):
        """Specialized version of _getObject, for replication"""
        r = self._getObject(oid, tid)
        if r:
            return r[:1] + r[2:] # remove next_serial

    def fetchObject(self, oid, tid):
        """
        Specialized version of getObject, for replication:
        - the oid can only be at an exact serial (parameter 'tid')
        - next_serial is not part of the result
        - if there's no result for the requested serial,
          no need check if oid exists at other serial
        """
        u64 = util.u64
        r = self._fetchObject(u64(oid), u64(tid))
        if r:
            serial, compression, checksum, data, data_serial = r
            return (util.p64(serial), compression, checksum, data,
                None if data_serial is None else util.p64(data_serial))

    def _getPartitionTable(self):
        """Return a whole partition table as a sequence of rows. Each row
        is again a tuple of an offset (row ID), the NID of a storage
        node, and a cell state."""

    @requires(_getPartitionTable)
    def _iterAssignedCells(self):
        my_nid = self.getUUID()
        return ((offset, tid) for offset, nid, tid in self._getPartitionTable()
                              if my_nid == nid)

    @requires(_getPartitionTable)
    def getPartitionTable(self):
        return [(offset, nid, max(0, -state))
            for offset, nid, state in self._getPartitionTable()]

    @contextmanager
    def replicated(self, offset):
        readable_set = self._readable_set
        assert offset not in readable_set
        readable_set.add(offset)
        try:
            yield
        finally:
            readable_set.remove(offset)

    def _changePartitionTable(self, cell_list, reset=False):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        the NID of a storage node, and a cell state. If reset is True,
        existing data is first thrown away.
        """

    def _getDataLastId(self, partition):
        """
        """

    @requires(_getDataLastId)
    def _updateReadable(self):
        try:
            readable_set = self.__dict__['_readable_set']
        except KeyError:
            readable_set = self._readable_set = set()
            np = self.getNumPartitions()
            def _getPartition(x, np=np):
                return x % np
            def _getReadablePartition(x, np=np, r=readable_set):
                x %= np
                if x in r:
                    return x
                raise NonReadableCell
            self._getPartition = _getPartition
            self._getReadablePartition = _getReadablePartition
            d = self._data_last_ids = []
            for p in xrange(np):
                i = self._getDataLastId(p)
                d.append(p << 48 if i is None else i + 1)
        else:
            readable_set.clear()
        readable_set.update(x[0] for x in self._iterAssignedCells()
                                 if -x[1] in READABLE)

    @requires(_changePartitionTable, _getLastIDs, _getLastTID)
    def changePartitionTable(self, ptid, cell_list, reset=False):
        my_nid = self.getUUID()
        pt = dict(self._iterAssignedCells())
        # In backup mode, the last transactions of a readable cell may be
        # incomplete.
        backup_tid = self.getBackupTID()
        if backup_tid:
            backup_tid = util.u64(backup_tid)
        def outofdate_tid(offset):
            tid = pt.get(offset, 0)
            if tid >= 0:
                return tid
            return -tid in READABLE and (backup_tid or
                max(self._getLastIDs(offset)[0],
                    self._getLastTID(offset))) or 0
        cell_list = [(offset, nid, (
                None if state == CellStates.DISCARDED else
                -state if nid != my_nid or state != CellStates.OUT_OF_DATE else
                outofdate_tid(offset)))
            for offset, nid, state in cell_list]
        self._changePartitionTable(cell_list, reset)
        self._updateReadable()
        assert isinstance(ptid, (int, long)), ptid
        self._setConfiguration('ptid', str(ptid))

    @requires(_changePartitionTable)
    def updateCellTID(self, partition, tid):
        t, = (t for p, t in self._iterAssignedCells() if p == partition)
        if t < 0:
            return
        tid = util.u64(tid)
        # Replicator doesn't optimize when there's no new data
        # since the node went down.
        if t == tid:
            return
        # In a backup cluster, when a storage node gets down just after
        # being the first to replicate fully new transactions from upstream,
        # we may end up in a special situation where an OUT_OF_DATE cell
        # is actually more up-to-date than an UP_TO_DATE one.
        assert t < tid or self.getBackupTID()
        self._changePartitionTable([(partition, self.getUUID(), tid)])

    def iterCellNextTIDs(self):
        p64 = util.p64
        backup_tid = self.getBackupTID()
        if backup_tid:
            next_tid = util.u64(backup_tid)
            if next_tid:
                next_tid += 1
        for offset, tid in self._iterAssignedCells():
            if tid >= 0: # OUT_OF_DATE
                yield offset, p64(tid and tid + 1)
            elif -tid in READABLE:
                if backup_tid:
                    # An UP_TO_DATE cell does not have holes so it's fine to
                    # resume from the last found records.
                    tid = self._getLastTID(offset)
                    yield offset, (
                        # For trans, a transaction can't be partially
                        # replicated, so replication can resume from the next
                        # possible tid.
                        p64(max(next_tid, tid + 1) if tid else next_tid),
                        # For obj, the last transaction may be partially
                        # replicated so it must be checked again (i.e. no +1).
                        p64(max(next_tid, self._getLastIDs(offset)[0])))
                else:
                    yield offset, None

    @abstract
    def dropPartitions(self, offset_list):
        """Delete all data for specified partitions"""

    def _getUnfinishedDataIdList(self):
        """Drop any unfinished data from a database."""

    @requires(_getUnfinishedDataIdList)
    def dropUnfinishedData(self):
        """Drop any unfinished data from a database."""
        data_id_list = self._getUnfinishedDataIdList()
        self.dropPartitionsTemporary()
        self.releaseData(data_id_list, True)
        self.commit()

    @abstract
    def dropPartitionsTemporary(self, offset_list=None):
        """Drop partitions from temporary tables"""

    @abstract
    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        """Write transaction metadata

        The list of objects contains tuples, each of which consists of
        an object ID, a data_id and object serial.
        The transaction is either None or a tuple of the list of OIDs,
        user information, a description, extension information and transaction
        pack state (True for packed).

        If 'temporary', the transaction is stored into ttrans/tobj tables,
        (instead of trans/obj). The caller is in charge of committing, which
        is always the case at tpc_vote.
        """

    @abstract
    def getOrphanList(self):
        """Return the list of data id that is not referenced by the obj table

        This is a repair method, and it's usually expensive.
        There was a bug that did not free data of transactions that were
        aborted before vote. This method is used to reclaim the wasted space.
        """

    @abstract
    def _pruneData(self, data_id_list):
        """To be overridden by the backend to delete any unreferenced data

        'unreferenced' means:
        - not in self._uncommitted_data
        - and not referenced by a fully-committed object (storage should have
          an index or a refcount of all data ids of all objects)

        The returned value is the number of deleted rows from the data table.
        """

    @abstract
    def storeData(self, checksum, oid, data, compression):
        """To be overridden by the backend to store object raw data

        If same data was already stored, the storage only has to check there's
        no hash collision.
        """

    @abstract
    def loadData(self, data_id):
        """Inverse of storeData
        """

    def holdData(self, checksum_or_id, *args):
        """Store raw data of temporary object

        If 'checksum_or_id' is a checksum, it must be the result of
        makeChecksum(data) and extra parameters must be (data, compression)
        where 'compression' indicates if 'data' is compressed.
        A volatile reference is set to this data until 'releaseData' is called
        with this checksum.
        If called with only an id, it only increment the volatile
        reference to the data matching the id.
        """
        if args:
            checksum_or_id = self.storeData(checksum_or_id, *args)
        self._uncommitted_data[checksum_or_id] += 1
        return checksum_or_id

    def releaseData(self, data_id_list, prune=False):
        """Release 1 volatile reference to given list of data ids

        If 'prune' is true, any data that is not referenced anymore (either by
        a volatile reference or by a fully-committed object) is deleted.
        """
        refcount = self._uncommitted_data
        for data_id in data_id_list:
            count = refcount[data_id] - 1
            if count:
                refcount[data_id] = count
            else:
                del refcount[data_id]
        if prune:
            return self._pruneData(data_id_list)

    @fallback
    def _getDataTID(self, oid, tid=None, before_tid=None):
        """
        Return a 2-tuple:
        tid (int)
            tid corresponding to received parameters
        serial
            data tid of the found record

        (None, None) is returned if requested object and transaction
        could not be found.

        This method only exists for performance reasons, by not returning data:
        _getObject already returns these values but it is slower.
        """
        r = self._getObject(oid, tid, before_tid)
        return (r[0], r[-1]) if r else (None, None)

    def findUndoTID(self, oid, tid, ltid, undone_tid, transaction_object):
        """
        oid
            Object OID
        tid
            Transation doing the undo
        ltid
            Upper (excluded) bound of transactions visible to transaction doing
            the undo.
        undone_tid
            Transaction to undo
        transaction_object
            Object data from memory, if it was modified by running
            transaction.
            None if is was not modified by running transaction.

        Returns a 3-tuple:
        current_tid (p64)
            TID of most recent version of the object client's transaction can
            see. This is used later to detect current conflicts (eg, another
            client modifying the same object in parallel)
        data_tid (int)
            TID containing (without indirection) the data prior to undone
            transaction.
            None if object doesn't exist prior to transaction being undone
              (its creation is being undone).
        is_current (bool)
            False if object was modified by later transaction (ie, data_tid is
            not current), True otherwise.
        """
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        tid = u64(tid)
        if ltid:
            ltid = u64(ltid)
        undone_tid = u64(undone_tid)
        def getDataTID(tid=None, before_tid=None):
            tid, data_tid = self._getDataTID(oid, tid, before_tid)
            current_tid = tid
            while data_tid:
                if data_tid < tid:
                    tid, data_tid = self._getDataTID(oid, data_tid)
                    if tid is not None:
                        continue
                logging.error("Incorrect data serial for oid %s at tid %s",
                              oid, current_tid)
                return current_tid, current_tid
            return current_tid, tid
        if transaction_object:
            try:
                current_tid = current_data_tid = u64(transaction_object[2])
            except struct.error:
                current_tid = current_data_tid = tid
        else:
            current_tid, current_data_tid = getDataTID(before_tid=ltid)
            if current_tid is None:
                return None, None, False
        found_undone_tid, undone_data_tid = getDataTID(tid=undone_tid)
        assert found_undone_tid is not None, (oid, undone_tid)
        is_current = undone_data_tid in (current_data_tid, tid)
        # Load object data as it was before given transaction.
        # It can be None, in which case it means we are undoing object
        # creation.
        _, data_tid = getDataTID(before_tid=undone_tid)
        if data_tid is not None:
            data_tid = p64(data_tid)
        return p64(current_tid), data_tid, is_current

    @abstract
    def lockTransaction(self, tid, ttid):
        """Mark voted transaction 'ttid' as committed with given 'tid'

        All pending changes are committed just before returning to the caller.
        """

    @abstract
    def unlockTransaction(self, tid, ttid, trans, obj):
        """Finalize a transaction by moving data to a finished area."""

    @abstract
    def abortTransaction(self, ttid):
        """"""

    @abstract
    def deleteTransaction(self, tid):
        """"""

    @abstract
    def deleteObject(self, oid, serial=None):
        """Delete given object. If serial is given, only delete that serial for
        given oid."""

    @abstract
    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        """Delete all objects and transactions between given min_tid (excluded)
        and max_tid (included)"""

    def truncate(self):
        tid = self.getTruncateTID()
        if tid:
            tid = util.u64(tid)
            assert tid, tid
            cell_list = []
            my_nid = self.getUUID()
            for partition, state in self._iterAssignedCells():
                if state > tid:
                    cell_list.append((partition, my_nid, tid))
                self._deleteRange(partition, tid)
            if cell_list:
                self._changePartitionTable(cell_list)
            self._setTruncateTID(None)
            self.commit()

    def repair(self, weak_app, dry_run):
        t = self._repairing
        if t and t.is_alive():
            logging.error('already repairing')
            return
        def repair():
            l = threading.Lock()
            l.acquire()
            def finalize():
                try:
                    if data_id_list and not dry_run:
                        self.commit()
                        logging.info("repair: deleted %s orphan records",
                                     self._pruneData(data_id_list))
                        self.commit()
                finally:
                    l.release()
            try:
                with self._duplicate() as db:
                    data_id_list = db.getOrphanList()
                logging.info("repair: found %s records that may be orphan",
                             len(data_id_list))
                weak_app().em.wakeup(finalize)
                l.acquire()
            finally:
                del self._repairing
            logging.info("repair: done")
        t = self._repairing = threading.Thread(target=repair)
        t.daemon = 1
        t.start()

    @abstract
    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""

    @abstract
    def getObjectHistory(self, oid, offset, length):
        """Return a list of serials and sizes for a given object ID.
        The length specifies the maximum size of such a list. Result starts
        with latest serial, and the list must be sorted in descending order.
        If there is no such object ID in a database, return None."""

    @abstract
    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        """Return a dict of length oids grouped by serial at (or above)
        min_tid and min_oid and below max_tid, for given partition,
        sorted in ascending order."""

    def _getTIDList(self, offset, length, partition_list):
        """Return a list of TIDs in ascending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""

    @requires(_getTIDList)
    def getTIDList(self, offset, length, partition_list):
        if partition_list:
            if self._readable_set.issuperset(partition_list):
                return map(util.p64, self._getTIDList(
                    offset, length, partition_list))
            raise NonReadableCell
        return ()

    @abstract
    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        """Return a list of TIDs in ascending order from an initial tid value,
        at most the specified length up to max_tid. The partition number is
        passed to filter out non-applicable TIDs."""

    @abstract
    def pack(self, tid, updateObjectDataForPack):
        """Prune all non-current object revisions at given tid.
        updateObjectDataForPack is a function called for each deleted object
        and revision with:
        - OID
        - packed TID
        - new value_serial
            If object data was moved to an after-pack-tid revision, this
            parameter contains the TID of that revision, allowing to backlink
            to it.
        - getObjectData function
            To call if value_serial is None and an object needs to be updated.
            Takes no parameter, returns a 3-tuple: compression, data_id,
            value
        """

    @abstract
    def checkTIDRange(self, partition, length, min_tid, max_tid):
        """
        Generate a digest from transaction list.
        min_tid (packed)
            TID at which verification starts.
        length (int)
            Maximum number of records to include in result.

        Returns a 3-tuple:
            - number of records actually found
            - a SHA1 computed from record's TID
              ZERO_HASH if no record found
            - biggest TID found (ie, TID of last record read)
              ZERO_TID if not record found
        """

    @abstract
    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        """
        Generate a digest from object list.
        min_oid (packed)
            OID at which verification starts.
        min_tid (packed)
            Serial of min_oid object at which search should start.
        length
            Maximum number of records to include in result.

        Returns a 5-tuple:
            - number of records actually found
            - a SHA1 computed from record's OID
              ZERO_HASH if no record found
            - biggest OID found (ie, OID of last record read)
              ZERO_OID if no record found
            - a SHA1 computed from record's serial
              ZERO_HASH if no record found
            - biggest serial found for biggest OID found (ie, serial of last
              record read)
              ZERO_TID if no record found
        """
