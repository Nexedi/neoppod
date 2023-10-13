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

import os, errno, socket, sys, thread, threading, weakref
from collections import defaultdict
from contextlib import contextmanager
from copy import copy
from time import time
from neo.lib import logging, util
from neo.lib.exception import NonReadableCell
from neo.lib.interfaces import abstract, requires
from neo.lib.protocol import CellStates, MAX_TID, ZERO_TID
from . import DatabaseFailure

READABLE = CellStates.UP_TO_DATE, CellStates.FEEDING

def fallback(func):
    setattr(Fallback, func.__name__, func)
    return abstract(func)

def splitOIDField(tid, oids):
    if len(oids) % 8:
        raise DatabaseFailure('invalid oids length for tid %s: %s'
            % (tid, len(oids)))
    return [oids[i:i+8] for i in xrange(0, len(oids), 8)]

class CreationUndone(Exception):
    pass

class Fallback(object):
    pass

class BackgroundWorker(object):

    _processing = None, None
    _exc_info = _thread = None
    _packing = _stop = False
    _orphan = _packed = None
    _pack_info = None,

    def __init__(self):
        self._stat_dict = {}
        self._drop_set = set()
        self._pack_set = set()

    def _delattr(self, *attrs):
        for attr in attrs:
            try:
                delattr(self, attr)
            except AttributeError:
                assert hasattr(self, attr)

    def _join(self, app, thread):
        l = app.em.lock
        l.release()
        thread.join()
        l.acquire()
        del self._thread
        self._delattr('_packing', '_processing', '_stop')
        exc_info = self._exc_info
        if exc_info:
            del self._exc_info
            etype, value, tb = exc_info
            raise etype, value, tb

    @contextmanager
    def _maybeResume(self, app):
        assert app.dm.lock._is_owned()
        if self._stop:
            self._join(app, self._thread)
        yield
        if app.operational and self._thread is None:
            t = self._thread = threading.Thread(
                name=self.__class__.__name__,
                target=self._worker,
                args=(weakref.ref(app),))
            t.daemon = 1
            t.start()

    @contextmanager
    def operational(self, app):
        assert app.em.lock.locked()
        try:
            with self._maybeResume(app):
                pass
            app.dm.lock.release()
            yield
        finally:
            thread = self._thread
            if thread is not None:
                self._stop = True
                logging.info("waiting for background tasks to interrupt")
                self._join(app, thread)
            locked = app.dm.lock.acquire(0)
            assert locked
            self._pack_set.clear()
            self._delattr('_pack_info', '_packed')

    def _stats(self, task, dm, what='deleted'):
        period = .01 if dm.LOCK else .1
        stats = self._stat_dict
        before = elapsed, count = stats.setdefault(task, (1e-99, 0))
        while True:
            start = time()
            # Do not process too few lines or the efficiency would drop.
            count += yield max(100, int(period * count / elapsed))
            elapsed += time() - start
            stats[task] = elapsed, count
            end = yield
            if end:
                break
        logging.info("%s (time: %ss/%ss, %s: %s/%s)",
            end, round(elapsed - before[0], 3), round(elapsed, 3),
            what, count - before[1], count)

    def _worker(self, weak_app):
        try:
            em_lock = weak_app().em.lock
            dm = dm2 = weak_app().dm
            try:
                mvcc = isinstance(dm, MVCCDatabaseManager)
                while True:
                    if mvcc:
                        stats = self._stats('prune', dm)
                        log = False
                        while True:
                            with em_lock:
                                # Tasks shall not leave uncommitted changes,
                                # so pass a dummy value as dm2 parameter
                                # to avoid a useless commit.
                                self._checkStop(dm, dm)
                            with dm.lock:
                                data_id_list = dm._dataIdsToPrune(next(stats))
                                if not data_id_list:
                                    break
                                if not log:
                                    logging.info(
                                        "deferred pruning: processing...")
                                    log = True
                                stats.send(dm._pruneData(data_id_list))
                                dm.commitFromTimeToTime()
                        if log:
                            stats.send(0)
                            try:
                                stats.send("deferred pruning: processed")
                            except StopIteration:
                                pass
                    with dm.lock:
                        if self._drop_set:
                            task = self._task_drop
                        elif self._pack_set:
                            task = self._task_pack
                            self._packing = True
                        elif self._orphan is not None:
                            task = self._task_orphan
                        else:
                            assert not dm.nonempty('todel')
                            self._stop = True
                        if self._stop:
                            break
                        if mvcc and dm is dm2:
                            # The following commit is actually useless for
                            # drop & orphan tasks. On the other hand, it is
                            # required if a 0-day pack is requested whereas
                            # the deferred commit for the latest tpc_finish
                            # affecting this node hasn't been processed yet.
                            dm.commit()
                            dm2 = copy(dm)
                    try:
                        task(weak_app, dm, dm2)
                    except DatabaseFailure, e:
                        e.checkTransientFailure(dm2)
                        with dm:
                            dm.commit()
                            dm2 = copy(dm)
            finally:
                dm is dm2 or dm2.close()
        except SystemExit:
            pass
        except:
            self._exc_info = sys.exc_info()
        finally:
            logging.info("background tasks stopped")
            thread = self._thread
            weak_app().em.wakeup(lambda: self._thread is thread
                and self._join(weak_app(), thread))

    def _checkStop(self, dm, dm2):
        # Either em or dm lock shall be locked.
        if self._stop:
            dm is dm2 or dm2.commit()
            with dm.lock:
                dm.commit()
            thread.exit()

    def _dm21(self, dm, dm2):
        if dm is dm2:
            return threading.Lock # faster than contextlib.nullcontext
        dm_lock = dm.lock
        dm2_commit = dm2.commit
        def dm21():
            dm2_commit()
            with dm_lock:
                yield
        return contextmanager(dm21)

    def _task_drop(self, weak_app, dm, dm2):
        stats = self._stats('drop', dm2)
        dropped = 0
        parts = self._drop_set
        em_lock = weak_app().em.lock
        dm21 = self._dm21(dm, dm2)
        while True:
            lock = threading.Lock()
            with lock:
                with em_lock:
                    try:
                        offset = min(parts) # same as in _task_pack
                    except ValueError:
                        if dropped:
                            try:
                                stats.send("%s partition(s) dropped" % dropped)
                            except StopIteration:
                                pass
                        break
                    self._processing = offset, lock
                logging.info("dropping partition %s...", offset)
                while True:
                    with em_lock:
                        self._checkStop(dm, dm2)
                        if offset not in parts: # partition reassigned
                            break
                    with dm2.lock:
                        deleted = dm2._dropPartition(offset, next(stats))
                        if type(deleted) is list:
                            try:
                                deleted.remove(None)
                                pass # XXX: not covered
                            except ValueError:
                                pass
                            pruned = dm2._pruneData(deleted)
                            stats.send(len(deleted) if pruned is None else
                                       pruned)
                        else:
                            stats.send(deleted)
                            if not deleted:
                                with dm21():
                                    try:
                                        parts.remove(offset)
                                    except KeyError:
                                        pass
                                    else:
                                        dropped += 1
                                        dm.commit()
                                break
                        dm2.commitFromTimeToTime()
            if dm is not dm2:
                # Process deferred pruning before dropping another partition.
                parts = ()

    def _task_pack(self, weak_app, dm, dm2):
        stats = self._stats('pack', dm2)
        pack_id, approved, partial, _oids, tid = self._pack_info
        assert approved, self._pack_info
        tid = util.u64(tid)
        packed = 0
        parts = self._pack_set
        em_lock = weak_app().em.lock
        dm21 = self._dm21(dm, dm2)
        while True:
            lock = threading.Lock()
            with lock:
                with em_lock, dm.lock:
                    try:
                        # Better than next(iter(...)) to resume work
                        # on a partially-processed partition.
                        offset = min(parts)
                    except ValueError:
                        if packed:
                            try:
                                stats.send(
                                    "%s partition(s) processed for pack %s"
                                    % (packed, util.dump(pack_id)))
                            except StopIteration:
                                pass
                            weak_app().notifyPackCompleted()
                        self._packing = False
                        if not (self._stop or self._pack_set):
                            weak_app().maybePack()
                        break
                    self._processing = offset, lock
                if partial:
                    np = dm.np
                    oid_index = 0
                    oids = [oid for oid in _oids if oid % np == offset]
                    logging.info(
                        "partial pack %s @%016x: partition %s (%s oids)...",
                        util.dump(pack_id), tid, offset, len(oids))
                else:
                    oid = -1
                    logging.info(
                        "pack %s @%016x: partition %s...",
                        util.dump(pack_id), tid, offset)
                while True:
                    with em_lock:
                        self._checkStop(dm, dm2)
                        if offset not in parts: # partition not readable anymore
                            break
                    with dm2.lock:
                        limit = next(stats)
                        if partial:
                            i = oid_index + limit
                            deleted = dm2._pack(offset,
                                oids[oid_index:i], tid)[1]
                            oid_index = i
                        else:
                            oid, deleted = dm2._pack(offset, oid+1, tid, limit)
                        stats.send(deleted)
                        if oid_index >= len(oids) if partial else oid is None:
                            with dm21():
                                try:
                                    parts.remove(offset)
                                except ValueError:
                                    pass
                                else:
                                    packed += 1
                                    i = util.u64(pack_id)
                                    assert dm._getPartitionPacked(offset) < i
                                    dm._setPartitionPacked(offset, i)
                                    dm.commit()
                            break
                        dm2.commitFromTimeToTime()
            if dm is not dm2:
                # Process deferred pruning before packing another partition.
                parts = ()

    def _task_orphan(self, weak_app, dm, dm2):
        dm21 = self._dm21(dm, dm2)
        logging.info("searching for orphan records...")
        with dm2.lock:
            data_id_list = dm2.getOrphanList()
            logging.info("found %s records that may be orphan",
                         len(data_id_list))
            if data_id_list and not self._orphan:
                deleted = dm2._pruneData(data_id_list)
                if deleted is not None:
                    logging.info("deleted %s orphan records", deleted)
                with dm21():
                    dm.commit()
            self._orphan = None

    def checkNotProcessing(self, app, offset, min_tid):
        assert offset not in self._drop_set, offset
        if offset in self._pack_set:
            # There are conditions to start packing when it's safe
            # (see filterPackable), so reciprocally we have the same condition
            # here to double check when it's safe to replicate during a pack.
            assert self._pack_info[0] < min_tid, (
                offset, min_tid, self._pack_info)
            return
        processing, lock = self._processing
        if processing == offset:
            if not lock.acquire(0):
                assert min_tid == ZERO_TID # newly assigned
                dm_lock = app.dm.lock
                em_lock = app.em.lock
                dm_lock.release()
                em_lock.release()
                lock.acquire()
                em_lock.acquire()
                dm_lock.acquire()
            lock.release()

    @contextmanager
    def dropPartitions(self, app):
        if app.disable_drop_partitions:
            drop_set = set()
            yield drop_set
            if drop_set:
                logging.info("don't drop data for partitions %r",
                             sorted(drop_set))
        else:
            with self._maybeResume(app):
                drop_set = self._drop_set
                yield drop_set
        self._pack_set -= drop_set

    def isReadyToStartPack(self):
        """
        If ready to start a pack, return 2-tuple:
        - last processed pack id (i.e. we already have all
          information to resume this pack), None otherwise
        - last completed pack for this storage node at the
          time of the last call to pack()
        Else return None.
        """
        if not (self._packing or self._pack_set):
            return self._pack_info[0], self._packed

    def pack(self, app, info, packed, offset_list):
        assert app.operational
        parts = self._pack_set
        assert not parts
        with self._maybeResume(app):
            parts.update(offset_list)
            if parts:
                if info:
                    pack_id, approved, partial, oids, tid = info
                    self._pack_info = (pack_id, approved, partial,
                        oids and map(util.u64, oids), tid)
                    self._packed = packed
                else:
                    assert self._packed == packed
            elif not packed:
                # Release memory: oids may take several MB.
                try:
                    del self._pack_info, self._packed
                except AttributeError:
                    pass

    def pruneOrphan(self, app, dry_run):
        with self._maybeResume(app):
            if self._orphan is None:
                self._orphan = dry_run
            else:
                logging.error('already repairing')


class DatabaseManager(object):
    """Base class for database managers

    It also describes the interface to be implemented.
    """

    ENGINES = ()
    TEST_IDENT = None
    UNSAFE = False

    __lockFile = None
    LOCK = "neostorage"
    LOCKED = "error: database is locked"

    _deferred_commit = 0
    _last_commit = 0
    _uncommitted_data = () # for secondary connections

    def __init__(self, database, engine=None, wait=None,
                 background_worker_class=BackgroundWorker):
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
        self._background_worker = background_worker_class()
        self.lock = threading.RLock()
        self.lock.acquire()
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
        self.lock = threading.RLock() # dummy lock
        self.lock.acquire()
        self._connect()

    _cached_attr_list = (
        'pt', '_readable_set', '_getPartition', '_getReadablePartition')

    def __getattr__(self, attr):
        if attr in self._cached_attr_list:
            self._updateReadable()
        return self.__getattribute__(attr)

    def __enter__(self):
        assert not self.LOCK, "not a secondary connection"
        # XXX: All config caching should be done in this class,
        #      rather than in backend classes.
        self._config.clear()
        try:
            for attr in self._cached_attr_list:
                delattr(self, attr)
        except AttributeError:
            pass

    def __exit__(self, t, v, tb):
        if v is None:
            # Deferring commits make no sense for secondary connections.
            assert not self._deferred_commit
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
        database.
        For other backends, there's no expected transient failure so the
        default implementation is to execute the given task exactly once.
        """
        assert not self.LOCK, "not a secondary connection"
        return f()

    def lockFile(self, db_path):
        if self.LOCK:
            assert self.__lockFile is None, self.__lockFile
            # For platforms that don't support anonymous sockets,
            # we can either use zc.lockfile or an empty SQLite db
            # (with BEGIN EXCLUSIVE).
            try:
                stat = os.stat(db_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                return # in-memory or temporary database
            s = self.__lockFile = socket.socket(socket.AF_UNIX)
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

    def restore(self, dump): # for tests
        self.erase()
        self._restore(dump)

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

    @property
    def operational(self):
        return self._background_worker.operational

    @property
    def checkNotProcessing(self):
        return self._background_worker.checkNotProcessing

    def _close(self):
        """Backend-specific code to close the database"""

    @requires(_close)
    def close(self):
        if self._deferred_commit:
            self.commit()
        self._close()
        if self.__lockFile:
            self.__lockFile.close()
            del self.__lockFile

    def _commit(self):
        """Backend-specific code to commit the pending changes"""

    @requires(_commit)
    def commit(self):
        assert self.lock._is_owned() or self.TEST_IDENT == thread.get_ident()
        logging.debug('committing...')
        self._commit()
        self._last_commit = time()
        # Instead of cancelling a timeout that would be set to defer a commit,
        # we simply use to a boolean so that _deferredCommit() does nothing.
        # IOW, epoll may wait wake up for nothing but that should be rare,
        # because most immediate commits are usually quickly followed by
        # deferred commits.
        self._deferred_commit = 0

    def deferCommit(self):
        self._deferred_commit = 1
        return self._deferredCommit

    def _deferredCommit(self):
        with self.lock:
            if self._deferred_commit:
                self.commit()

    def commitFromTimeToTime(self, period=1):
        if self._last_commit + period < time():
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

    def _changePartitionTable(self, cell_list, reset=False):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        the NID of a storage node, and a cell state. If reset is True,
        existing data is first thrown away.
        """

    def _getPartitionTable(self):
        """Return a whole partition table as a sequence of rows. Each row
        is again a tuple of an offset (row ID), the NID of a storage node,
        either a tid or the negative of a cell state, and a pack id.
        """

    def getUUID(self):
        """
            Load a NID from a database.
        """
        nid = self.getConfiguration('nid')
        if nid is not None:
            return int(nid)

    @requires(_changePartitionTable, _getPartitionTable)
    def setUUID(self, nid):
        """
            Store a NID into a database.
        """
        old_nid = self.getUUID()
        if nid != old_nid:
            if old_nid:
                self._changePartitionTable((offset, x, tid, pack)
                    for offset, x, tid, pack in self._getPartitionTable()
                    if x == old_nid
                    for x, tid in ((x, None), (nid, tid)))
            self.setConfiguration('nid', str(nid))

    def getNumReplicas(self):
        """
            Load the number of replicas from a database.
        """
        n = self.getConfiguration('replicas')
        if n is not None:
            return int(n)

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
        x = self._readable_set
        if x:
            return max(self._getLastTID(x, max_tid) for x in x)

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
            tid = max(self.getLastTID(), max(tid))
            oid = max(oid)
            return (None if tid is None else util.p64(tid),
                    None if oid is None else util.p64(oid))
        return None, None

    def _getPackOrders(self, min_completed):
      """Request list of pack orders excluding oldest completed ones.

      Return information from pack orders with id >= min_completed,
      only from readable partitions. As a iterable of:
      - pack id (int)
      - approved (None if not signed, else cast as boolean)
      - partial (cast as boolean)
      - oids (list of 8-byte strings)
      - pack tid (int)
      """

    @requires(_getPackOrders)
    def getPackOrders(self, min_completed):
        p64 = util.p64
        return [(
            p64(id),
            None if approved is None else bool(approved),
            bool(partial),
            oids,
            p64(tid),
        ) for id, approved, partial, oids, tid in self._getPackOrders(
            util.u64(min_completed))]

    @abstract
    def getPackedIDs(self, up_to_date=False):
        """Return pack status of assigned partitions

        Return {offset: pack_id (as 8-byte)}
        If up_to_date, returned dict shall only contain information
        about UP_TO_DATE partitions.
        """

    @abstract
    def _getPartitionPacked(self, partition):
        """Get the last completed pack (id as int) for an assigned partition"""

    @abstract
    def _setPartitionPacked(self, partition, pack_id):
        """Set the last completed pack (id as int) for an assigned partition"""

    def updateCompletedPackByReplication(self, partition, pack_id):
        """
        The caller is going to replicate data from another node that may have
        already packed objects and we must adjust our pack status so that we
        don't do process too many or too few packs.

        pack_id (as 8-byte) is the last completed pack id on the feeding nodes
        so that must also be ours now if our last completed pack is more recent,
        which means we'll have to redo some packs.
        """
        pack_id = util.u64(pack_id)
        if pack_id < self._getPartitionPacked(partition):
            self._setPartitionPacked(partition, pack_id)

    @property
    def pack(self):
        return self._background_worker.pack

    @property
    def isReadyToStartPack(self):
        return self._background_worker.isReadyToStartPack

    @property
    def repair(self):
        return self._background_worker.pruneOrphan

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
    @requires(_getObject)
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

    @requires(_getPartitionTable)
    def iterAssignedCells(self):
        my_nid = self.getUUID()
        return ((offset, tid)
            for offset, nid, tid, pack in self._getPartitionTable()
            if my_nid == nid)

    @requires(_getPartitionTable)
    def getPartitionTable(self):
        return [(offset, nid, max(0, -tid))
            for offset, nid, tid, pack in self._getPartitionTable()]

    @contextmanager
    def replicated(self, offset):
        readable_set = self._readable_set
        assert offset not in readable_set
        readable_set.add(offset)
        try:
            yield
        finally:
            readable_set.remove(offset)

    def _getDataLastId(self, partition):
        """
        """

    def _getMaxPartition(self):
        """
        """

    @requires(_getDataLastId, _getMaxPartition)
    def _updateReadable(self, reset=True):
        if reset:
            readable_set = self._readable_set = set()
            np = self.np = 1 + self._getMaxPartition()
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
            readable_set = self._readable_set
            readable_set.clear()
        readable_set.update(x[0] for x in self.iterAssignedCells()
                                 if -x[1] in READABLE)

    @requires(_changePartitionTable, _getLastIDs, _getLastTID)
    def changePartitionTable(self, app, ptid, num_replicas, cell_list,
                             reset=False):
        my_nid = self.getUUID()
        pt = dict(self.iterAssignedCells())
        # In backup mode, the last transactions of a readable cell may be
        # incomplete.
        backup_tid = self.getBackupTID()
        if backup_tid:
            backup_tid = util.u64(backup_tid)
        max_offset = -1
        assigned = []
        cells = []
        pack_set = self._background_worker._pack_set
        app_last_pack_id = util.u64(app.last_pack_id)
        with self._background_worker.dropPartitions(app) as drop_set:
            for offset, nid, state in cell_list:
                if max_offset < offset:
                    max_offset = offset
                pack = None
                if state == CellStates.DISCARDED:
                    if nid == my_nid:
                        drop_set.add(offset)
                        pack_set.discard(offset)
                    tid = None
                else:
                    if nid == my_nid:
                        assigned.append(offset)
                        if state in READABLE:
                            assert not (app_last_pack_id and reset), (
                                reset, app_last_pack_id, cell_list)
                            pack = 0
                        else:
                            pack_set.discard(offset)
                            pack = app_last_pack_id
                    if nid != my_nid or state != CellStates.OUT_OF_DATE:
                        tid = -state
                    else:
                        tid = pt.get(offset, 0)
                        if tid < 0:
                            tid = -tid in READABLE and (backup_tid or
                                max(self._getLastIDs(offset)[0],
                                    self._getLastTID(offset))) or 0
                cells.append((offset, nid, tid, pack))
            if reset:
                drop_set.update(xrange(max_offset + 1))
            drop_set.difference_update(assigned)
            self._changePartitionTable(cells, reset)
            self._updateReadable(reset)
            assert isinstance(ptid, (int, long)), ptid
            self._setConfiguration('ptid', str(ptid))
            self._setConfiguration('replicas', str(num_replicas))

    @requires(_changePartitionTable)
    def updateCellTID(self, partition, tid):
        t, = (t for p, t in self.iterAssignedCells() if p == partition)
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
        self._changePartitionTable([(partition, self.getUUID(), tid, None)])

    def iterCellNextTIDs(self):
        p64 = util.p64
        backup_tid = self.getBackupTID()
        if backup_tid:
            next_tid = util.u64(backup_tid)
            if next_tid:
                next_tid += 1
        for offset, tid in self.iterAssignedCells():
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
    def _dropPartition(self, offset, count):
        """Delete rows for given partition

        Delete at most 'count' rows of from obj:
        - if there's no line to delete, purge trans and return
          a boolean indicating if any row was deleted (from trans)
        - else return data ids of deleted rows
        """

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

        When called by a secondary connection, the method must only add
        data_id_list to the 'todel' table (see MVCCDatabaseManager) and
        return None.
        """

    @abstract
    def storeData(self, checksum, oid, data, compression, data_tid):
        """To be overridden by the backend to store object raw data

        'checksum' must be the result of makeChecksum(data).
        'compression' indicates if 'data' is compressed.
        In the case of undo, 'data_tid' may not be None:
        - if (oid, data_tid) exists, the related data_id must be returned;
        - else, if it can happen (e.g. cell is not readable), the caller
          must have passed valid (checksum, data, compression) as fallback.

        If same data was already stored, the storage only has to check there's
        no hash collision.
        """

    @abstract
    def loadData(self, data_id):
        """Inverse of storeData
        """

    def holdData(self, *args):
        """Store and hold data

        The parameters are same as storeData.
        A volatile reference is set to this data until 'releaseData' is called.
        """
        data_id = self.storeData(*args)
        if data_id is not None:
            self._uncommitted_data[data_id] += 1
            return data_id

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
            self._pruneData(data_id_list)

    def _getObjectHistoryForUndo(self, oid, undo_tid):
        """Return (undone_tid, history) where 'undone_tid' is the greatest tid
        before 'undo_tid' and 'history' is the list of (tid, value_tid) after
        'undo_tid'. If there's no record at 'undo_tid', return None."""

    @requires(_getObjectHistoryForUndo)
    def findUndoTID(self, oid, ltid, undo_tid, current_tid):
        """
        oid
            Object OID
        tid
            Transation doing the undo
        ltid
            Upper (excluded) bound of transactions visible to transaction doing
            the undo.
        undo_tid
            Transaction to undo
        current_tid
            Serial of object data from memory, if it was modified by running
            transaction. None otherwise.

        Returns a 3-tuple:
        current_tid (p64)
            TID of most recent version of the object client's transaction can
            see. This is used later to detect current conflicts (eg, another
            client modifying the same object in parallel)
        data_tid (int)
            TID containing the data prior to undone transaction.
            None if object doesn't exist prior to transaction being undone
              (its creation is being undone).
        is_current (bool)
            False if object was modified by later transaction (ie, data_tid is
            not current), True otherwise.

        When undoing several times in such a way that several data_tid are
        possible, the implementation guarantees to return the greatest one,
        which makes undo compatible with pack without having to update the
        value_tid of obj records. IOW, all records that are undo-identical
        constitute a simply-linked list; after a pack, the value_tid of the
        record with the smallest TID points to nowhere.

        With a different implementation, it could fail as follows:
            tid value_tid
            10  -
            20  10
            30  10
            40  20
        After packing at 30, the DB would lose the information that 30 & 40
        are undo-identical.

        TODO: Since ZODB requires nothing about how undo-identical records are
              linked, imported databases may not be packable without breaking
              undo information. Same for existing databases because older NEO
              implementation linked records differently. A background task to
              fix value_tid should be implemented; for example, it would be
              used automatically once Importer has finished, if it has seen
              non-null value_tid.
        """
        u64 = util.u64
        undo_tid = u64(undo_tid)
        history = self._getObjectHistoryForUndo(u64(oid), undo_tid)
        if not history:
            return # nothing to undo for this oid at undo_tid
        undone_tid, history = history
        if current_tid:
            current = u64(current_tid)
        else:
            ltid = u64(ltid) if ltid else float('inf')
            for current, _ in reversed(history):
                if current < ltid:
                    break
            else:
                if ltid <= undo_tid:
                    return None, None, False
                current = undo_tid
            current_tid = util.p64(current)
        is_current = current == undo_tid
        for tid, data_tid in history:
            if data_tid is not None:
                if data_tid == undone_tid:
                    undone_tid = tid
                elif data_tid == undo_tid:
                    if current == tid:
                        is_current = True
                    else:
                        undo_tid = tid
        return (current_tid,
                None if undone_tid is None else util.p64(undone_tid),
                is_current)

    @abstract
    def storePackOrder(self, tid, approved, partial, oid_list, pack_tid):
        """Store a pack order

        - tid (8-byte)
            pack id
        - approved
            not signed (None), rejected (False) or approved (True)
        - partial (boolean)
        - oid_list (list of 8-byte)
        - pack_tid (8-byte)
        """

    def _signPackOrders(self, approved, rejected):
        """Update signing status of pack orders

        Both parameters are lists of pack ids as int.
        Return list of pack orders (ids as int) that could be updated.
        """

    @requires(_signPackOrders)
    def signPackOrders(self, approved, rejected, auto_commit=True):
        u64 = util.u64
        changed = map(util.p64, self._signPackOrders(
            map(u64, approved), map(u64, rejected)))
        if changed:
            if auto_commit:
                self.commit()
            def _(signed):
                signed = set(signed)
                signed.difference_update(changed)
                return sorted(signed)
            return _(approved), _(rejected)
        return approved, rejected

    @abstract
    def lockTransaction(self, tid, ttid, pack):
        """Mark voted transaction 'ttid' as committed with given 'tid'

        All pending changes are committed just before returning to the caller.
        """

    @abstract
    def unlockTransaction(self, tid, ttid, trans, obj, pack):
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
            for partition, state in self.iterAssignedCells():
                if state > tid:
                    cell_list.append((partition, my_nid, tid, None))
                self._deleteRange(partition, tid)
                self.commitFromTimeToTime(10)
            if cell_list:
                self._changePartitionTable(cell_list)
            self._setTruncateTID(None)
            self.commit()

    @abstract
    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""

    @abstract
    def getObjectHistoryWithLength(self, oid, offset, length):
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
    def _pack(self, offset, oid, tid, limit=None):
        """
        The undo feature is implemented in such a way that value_tid does not
        have to be updated. This is important for performance reasons, but also
        because pack must be idempotent to guarantee that up-to-date replicas
        are identical.
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


class MVCCDatabaseManager(DatabaseManager):
    """Base class for MVCC database managers

    Which means when it can work efficiently with several concurrent
    connections to the underlying database.

    An extra 'todel' table is needed to defer data pruning by secondary
    connections.
    """

    @abstract
    def _dataIdsToPrune(self, limit):
        """Iterate over the 'todel' table

        Return the next ids to be passed to '_pruneData'. 'limit' specifies
        the maximum number of ids to return.

        Because deleting rows gradually can be inefficient, it's always called
        again until it returns no id at all, without any concurrent task that
        could add new ids. This way, the database manager can just:
        - remember the last greatest id returned (it does not have to
          persistent, i.e. it should be fast enough to restart from the
          beginning if it's interrupted);
        - and recreate the table on the last call.

        When returning no id whereas it previously returned ids,
        the method must commit.
        """
