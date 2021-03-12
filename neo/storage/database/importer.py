#
# Copyright (C) 2014-2019  Nexedi SA
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

import os
import pickle, sys, time
from bisect import bisect, insort
from collections import deque
from cStringIO import StringIO
from ConfigParser import SafeConfigParser
from ZConfig import loadConfigFile
from ZODB import BaseStorage
from ZODB._compat import dumps, loads, _protocol, PersistentPickler
from ZODB.config import getStorageSchema, storageFromString
from ZODB.POSException import POSKeyError
from ZODB.FileStorage import FileStorage

from ..app import option_defaults
from . import buildDatabaseManager, DatabaseFailure
from .manager import DatabaseManager, Fallback
from neo.lib import compress, logging, patch, util
from neo.lib.interfaces import implements
from neo.lib.protocol import BackendNotImplemented, MAX_TID

patch.speedupFileStorageTxnLookup()

FORK = sys.platform != 'win32'

def transactionAsTuple(txn):
    ext = txn.extension
    return (txn.user, txn.description,
        dumps(ext, _protocol) if ext else '',
        txn.status == 'p', txn.tid)

@apply
def patch_save_reduce(): # for _noload.__reduce__
    Pickler = PersistentPickler(None, StringIO()).__class__
    try:
        orig_save_reduce = Pickler.save_reduce.__func__
    except AttributeError: # both cPickle and C zodbpickle accept
        return             # that first reduce argument is None
    BUILD = pickle.BUILD
    REDUCE = pickle.REDUCE
    def save_reduce(self, func, args, state=None,
                    listitems=None, dictitems=None, obj=None):
        if func is not None:
            return orig_save_reduce(self,
                func, args, state, listitems, dictitems, obj)
        assert args is ()
        save = self.save
        write = self.write
        save(func)
        save(args)
        self.write(REDUCE)
        if obj is not None:
            self.memoize(obj)
        self._batch_appends(listitems)
        self._batch_setitems(dictitems)
        if state is not None:
            save(state)
            write(BUILD)
    Pickler.save_reduce = save_reduce


class Reference(object):

    __slots__ = "value",
    def __init__(self, value):
        self.value = value


class Repickler(pickle.Unpickler):

    def __init__(self, persistent_map):
        self._f = StringIO()
        # Use python implementation for unpickling because loading can not
        # be customized enough with cPickle.
        pickle.Unpickler.__init__(self, self._f)
        def persistent_id(obj):
            if isinstance(obj, Reference):
                r = obj.value
                del obj.value # minimize refcnt like for deque+popleft
                return r
        # For pickling, it is possible to use the fastest implementation,
        # which also generates fewer useless PUT opcodes.
        self._p = PersistentPickler(persistent_id, self._f, 1)
        self.memo = self._p.memo # just a tiny optimization

        def persistent_load(obj):
            new_obj = persistent_map(obj)
            if new_obj is not obj:
                self._changed = True
            return Reference(new_obj)
        self.persistent_load = persistent_load

    def _save(self, data):
        self._p.dump(data.popleft())
        # remove STOP (no need to truncate since it will always be overridden)
        self._f.seek(-1, 1)

    def __call__(self, data):
        f = self._f
        f.truncate(0)
        f.write(data)
        f.reset()
        self._changed = False
        try:
            classmeta = self.load()
            state = self.load()
        finally:
            self.memo.clear()
        if self._changed:
            f.truncate(0)
            dump = self._p.dump
            try:
                dump(classmeta)
                dump(state)
            finally:
                self.memo.clear()
            return f.getvalue()
        return data

    dispatch = pickle.Unpickler.dispatch.copy()

    class _noload(object):

        state = None

        def __new__(cls, dump):
            def load(*args):
                self = object.__new__(cls)
                self.dump = dump
                # We use deque+popleft everywhere to minimize the number of
                # references at the moment cPickle considers memoizing an
                # object. This reduces the number of useless PUT opcodes and
                # usually produces smaller pickles than ZODB. Without this,
                # they would, on the contrary, increase in size.
                # We could also use optimize from pickletools module.
                self.args = deque(args)
                self._list = deque()
                self.append = self._list.append
                self.extend = self._list.extend
                self._dict = deque()
                return self
            return load

        def __setitem__(self, *args):
            self._dict.append(args)

        def dict(self):
            while self._dict:
                yield self._dict.popleft()

        def list(self, pos):
            pt = self.args.popleft()
            f = pt._f
            f.seek(pos + 3) # NONE + EMPTY_TUPLE + REDUCE
            put = f.read()  # preserve memo if any
            f.truncate(pos)
            f.write(self.dump(pt, self.args) + put)
            while self._list:
                yield self._list.popleft()

        def __reduce__(self):
            return None, (), self.state, \
              self.list(self.args[0]._f.tell()), self.dict()

    @_noload
    def _obj(self, args):
        self._f.write(pickle.MARK)
        while args:
            self._save(args)
        return pickle.OBJ

    def _instantiate(self, klass, k):
        args = self.stack[k+1:]
        self.stack[k:] = self._obj(klass, *args),

    del dispatch[pickle.NEWOBJ] # ZODB < 5 has never used protocol 2

    @_noload
    def find_class(self, args):
        module, name = args
        return pickle.GLOBAL + module + '\n' + name + '\n'

    @_noload
    def _reduce(self, args):
        self._save(args)
        self._save(args)
        return pickle.REDUCE

    def load_reduce(self):
        stack = self.stack
        args = stack.pop()
        stack[-1] = self._reduce(stack[-1], args)
    dispatch[pickle.REDUCE] = load_reduce

    def load_build(self):
        stack = self.stack
        state = stack.pop()
        inst = stack[-1]
        assert inst.state is None
        inst.state = state
    dispatch[pickle.BUILD] = load_build


class ZODB(object):

    def __init__(self, storage, oid=0, **kw):
        self.oid = int(oid)
        self.mountpoints = {k: int(v) for k, v in kw.iteritems()}
        self.connect(storage)
        self.ltid = util.u64(self.lastTransaction())
        if not self.ltid:
            raise DatabaseFailure("Can not import empty storage: %s" % storage)
        self.mapping = {}

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_connect"], state["data_tid"], state["storage"]
        return state

    def connect(self, storage):
        self.data_tid = {}
        config, _ = loadConfigFile(getStorageSchema(), StringIO(storage))
        section = config.storage
        def _connect():
            self.storage = section.open()
        self._connect = _connect
        config = section.config
        if 'read_only' in config.getSectionAttributes():
            has_next_oid = config.read_only = 'next_oid' in self.__dict__
            if not has_next_oid:
                import gc
                # This will reopen read-only as soon as we know the last oid.
                def new_oid():
                    del self.new_oid
                    new_oid = self.storage.new_oid()
                    self.storage.close()
                    # A FileStorage index can be huge, and close() does not
                    # delete it. Stop reference it before loading it again,
                    # to avoid having it twice in memory.
                    del self.storage
                    gc.collect()  # to be sure (maybe only required for PyPy,
                                  #             if one day we support it)
                    config.read_only = True
                    self._connect()
                    return new_oid
                self.new_oid = new_oid
        self._connect()

    def setup(self, zodb_dict, shift_oid=0):
        self.shift_oid = shift_oid
        self.next_oid = util.u64(self.new_oid())
        shift_oid += self.next_oid
        for mp, oid in self.mountpoints.iteritems():
            mp = zodb_dict[mp]
            new_oid = mp.oid
            try:
                new_oid += mp.shift_oid
            except AttributeError:
                new_oid += shift_oid
                shift_oid = mp.setup(zodb_dict, shift_oid)
            self.mapping[oid] = new_oid
        del self.mountpoints
        return shift_oid

    def repickle(self, data):
        if not (self.shift_oid or self.mapping):
            self.repickle = lambda x: x
            return data
        u64 = util.u64
        p64 = util.p64
        def map_oid(obj):
            if isinstance(obj, tuple) and len(obj) == 2:
                oid = u64(obj[0])
                # If this oid pointed to a mount point, drop 2nd item because
                # it's probably different than the real class of the new oid.
            elif isinstance(obj, bytes):
                oid = u64(obj)
            else:
                raise NotImplementedError(
                    "Unsupported external reference: %r" % obj)
            try:
                return p64(self.mapping[oid])
            except KeyError:
                if not self.shift_oid:
                    return obj # common case for root db
            oid = p64(oid + self.shift_oid)
            return oid if isinstance(obj, bytes) else (oid, obj[1])
        self.repickle = Repickler(map_oid)
        return self.repickle(data)

    def __getattr__(self, attr):
        if attr == '__setstate__':
            return object.__getattribute__(self, attr)
        return getattr(self.storage, attr)

    def getDataTid(self, oid, tid):
        try:
            return self.data_tid[tid].get(oid)
        except KeyError:
            assert tid not in self.data_tid, (oid, tid)
            p_tid = util.p64(tid)
            txn = next(self.storage.iterator(p_tid))
            if txn.tid != p_tid:
                raise
        u64 = util.u64
        txn = self.data_tid[tid] = {
            u64(x.oid): x.data_txn
            for x in txn if x.data_txn}
        return txn.get(oid)


class ZODBIterator(object):

    def __new__(cls, zodb_list, *args):
        def _init(zodb):
            self = object.__new__(cls)
            iterator = zodb.iterator(*args)
            def _next():
                self.transaction = next(iterator)
            self.zodb = zodb
            self.next = _next
            return self
        def init(zodb):
            # FileStorage is fork-safe and in case we don't start iteration
            # from the beginning, we want the tid index built at most once
            # (by speedupFileStorageTxnLookup).
            if FORK and not isinstance(zodb.storage, FileStorage):
                def init():
                    zodb._connect()
                    return _init(zodb)
                return init
            return _init(zodb)
        def result(zodb_list):
            for self in zodb_list:
                if callable(self):
                    self = self()
                try:
                    self.next()
                    yield self
                except StopIteration:
                    pass
        return result(map(init, zodb_list))

    tid = property(lambda self: self.transaction.tid)

    def __lt__(self, other):
        return self.tid < other.tid or self.tid == other.tid \
            and self.zodb.shift_oid < other.zodb.shift_oid


is_true = ('false', 'true').index

class ImporterDatabaseManager(DatabaseManager):
    """Proxy that transparently imports data from a ZODB storage
    """
    _writeback = None
    _last_commit = 0

    def __init__(self, *args, **kw):
        super(ImporterDatabaseManager, self).__init__(*args, **kw)
        implements(self, """_getNextTID checkSerialRange checkTIDRange
            deleteObject deleteTransaction dropPartitions _getLastTID
            getReplicationObjectList _getTIDList nonempty""".split())

    _getPartition = property(lambda self: self.db._getPartition)
    _getReadablePartition = property(lambda self: self.db._getReadablePartition)
    _uncommitted_data = property(lambda self: self.db._uncommitted_data)

    def _parse(self, database):
        config = SafeConfigParser()
        config.read(os.path.expanduser(database))
        sections = config.sections()
        main = self._conf = option_defaults.copy()
        main.update(config.items(sections.pop(0)))
        self.zodb = [(x, dict(config.items(x))) for x in sections]
        x = main.get('compress', 'true')
        try:
            self.compress = bool(is_true(x))
        except ValueError:
            self.compress = compress.parseOption(x)
        if is_true(main.get('writeback', 'false')):
            if len(self.zodb) > 1:
                raise Exception(
                    "Can not forward new transactions to splitted DB.")
            self._writeback = self.zodb[0][1]['storage']

    def _connect(self):
        conf = self._conf
        db = self.db = buildDatabaseManager(conf['adapter'],
            (conf['database'], conf.get('engine'), conf['wait']))
        for x in """getConfiguration _setConfiguration _getMaxPartition
                    query erase getPartitionTable iterAssignedCells
                    updateCellTID getUnfinishedTIDDict dropUnfinishedData
                    abortTransaction storeTransaction lockTransaction
                    loadData storeData getOrphanList _pruneData deferCommit
                    _getDevPath dropPartitionsTemporary
                 """.split():
            setattr(self, x, getattr(db, x))
        if self._writeback:
            self._writeback = WriteBack(db, self._writeback)
        db_commit = db.commit
        def commit():
            db_commit()
            self._last_commit = time.time()
            if self._writeback:
                self._writeback.committed()
        self.commit = db.commit = commit

    def _updateReadable(*_):
        raise AssertionError

    def setUUID(self, nid):
        old_nid = self.getUUID()
        if old_nid:
            assert old_nid == nid, (old_nid, nid)
        else:
            self.setConfiguration('nid', str(nid))

    def changePartitionTable(self, *args, **kw):
        self.db.changePartitionTable(*args, **kw)
        if self._writeback:
            self._writeback.changed()

    def unlockTransaction(self, *args):
        self.db.unlockTransaction(*args)
        if self._writeback:
            self._writeback.changed()

    def close(self):
        if self._writeback:
            self._writeback.close()
        self.db.close()
        if isinstance(self.zodb, tuple): # _setup called
            for zodb in self.zodb:
                zodb.close()

    def setup(self, reset=False, dedup=False):
        self.db.setup(reset, dedup)
        zodb_state = self.getConfiguration("zodb")
        if zodb_state:
            logging.warning("Ignoring configuration file for oid mapping."
                            " Reloading it from NEO storage.")
            zodb = loads(zodb_state)
            for k, v in self.zodb:
                zodb[k].connect(v["storage"])
        else:
            zodb = {k: ZODB(**v) for k, v in self.zodb}
            x, = (x for x in zodb.itervalues() if not x.oid)
            x.setup(zodb)
            self.setConfiguration("zodb", dumps(zodb))
        self.zodb_index, self.zodb = zip(*sorted(
            (x.shift_oid, x) for x in zodb.itervalues()))
        self.zodb_ltid = max(x.ltid for x in self.zodb)
        zodb = self.zodb[-1]
        self.zodb_loid = zodb.shift_oid + zodb.next_oid - 1
        self.zodb_tid = self._getMaxPartition() is not None and \
            self.db.getLastTID(self.zodb_ltid) or 0
        if callable(self._import): # XXX: why ?
            if self.zodb_tid == self.zodb_ltid:
                self._finished()
            else:
                self._import = self._import()

    def doOperation(self, app):
        if self._import:
            app.newTask(self._import)

    def _import(self):
        p64 = util.p64
        u64 = util.u64
        tid = p64(self.zodb_tid + 1) if self.zodb_tid else None
        zodb_list = ZODBIterator(self.zodb, tid, p64(self.zodb_ltid))
        if FORK:
            from multiprocessing import Process
            from ..shared_queue import Queue
            queue = Queue(1<<24)
            process = self._import_process = Process(
                target=lambda zodb_list: queue(self._iter_zodb(zodb_list)),
                args=(zodb_list,))
            process.daemon = True
            process.start()
        else:
            queue = self._iter_zodb(zodb_list)
            process = None
        del zodb_list
        object_list = []
        data_id_list = []
        for txn in queue:
            if txn is None:
                break
            if len(txn) == 3:
                oid, data_id, data_tid = txn
                checksum, data, compression = data_id or (None, None, 0)
                data_id = self.holdData(
                    checksum, oid, data, compression, data_tid)
                data_id_list.append(data_id)
                object_list.append((oid, data_id, data_tid))
                # Give the main loop the opportunity to process requests
                # from other nodes. In particular, clients may commit. If the
                # storage node exits after such commit, and before we actually
                # update 'obj' with 'object_list', some rows in 'data' may be
                # unreferenced. This is not a problem because the leak is
                # solved when resuming the migration.
                # XXX: The leak was solved by the deduplication,
                #      but it was disabled by default.
            else: # len(txn) == 5
                tid = txn[-1]
                self.storeTransaction(tid, object_list,
                    ((x[0] for x in object_list),) + txn,
                    False)
                self.releaseData(data_id_list)
                logging.debug("TXN %s imported (user=%r, desc=%r, len(oid)=%s)",
                    util.dump(tid), txn[0], txn[1], len(object_list))
                del object_list[:], data_id_list[:]
                if self._last_commit + 1 < time.time():
                    self.commit()
                self.zodb_tid = u64(tid)
            yield
        if process:
            process.join()
        self.commit()
        self._finished()

    def _finished(self):
        logging.warning("All data are imported. You should change"
            " your configuration to use the native backend and restart.")
        self._import = None
        for x in """getObject getReplicationTIDList getReplicationObjectList
                    _fetchObject _getDataTID getLastObjectTID
                 """.split():
            setattr(self, x, getattr(self.db, x))
        for zodb in self.zodb:
            zodb.close()
        self.zodb = None

    def _iter_zodb(self, zodb_list):
        util.setproctitle('neostorage: import')
        p64 = util.p64
        u64 = util.u64
        zodb_list = list(zodb_list)
        if zodb_list:
            tid = None
            _compress = compress.getCompress(self.compress)
            while 1:
                zodb_list.sort()
                z = zodb_list[0]
                # Merge transactions with same tid. Only
                # user/desc/ext from first ZODB are kept.
                if tid != z.tid:
                    if tid:
                        yield txn
                    txn = transactionAsTuple(z.transaction)
                    tid = txn[-1]
                zodb = z.zodb
                for r in z.transaction:
                    oid = p64(u64(r.oid) + zodb.shift_oid)
                    data_tid = r.data_txn
                    if data_tid or r.data is None:
                        data = None
                    else:
                        _, compression, data = _compress(zodb.repickle(r.data))
                        data = util.makeChecksum(data), data, compression
                    yield oid, data, data_tid
                try:
                    z.next()
                except StopIteration:
                    del zodb_list[0]
                    if not zodb_list:
                        break
            yield txn
        yield

    def inZodb(self, oid, tid=None, before_tid=None):
        return oid <= self.zodb_loid and (
            self.zodb_tid < before_tid if before_tid else
            tid is None or self.zodb_tid < tid <= self.zodb_ltid)

    def zodbFromOid(self, oid):
        zodb = self.zodb[bisect(self.zodb_index, oid) - 1]
        return zodb, oid - zodb.shift_oid

    def getLastIDs(self):
        tid, oid = self.db.getLastIDs()
        return (max(tid, util.p64(self.zodb_ltid)),
                max(oid, util.p64(self.zodb_loid)))

    def _getObject(self, oid, tid=None, before_tid=None):
        p64 = util.p64
        r = self.getObject(p64(oid),
            None if tid is None else p64(tid),
            None if before_tid is None else p64(before_tid))
        if r:
            serial, next_serial, compression, checksum, data, data_serial = r
            u64 = util.u64
            return (u64(serial),
                    next_serial and u64(next_serial),
                    compression, checksum, data,
                    data_serial and u64(data_serial))

    def getObject(self, oid, tid=None, before_tid=None):
        u64 = util.u64
        u_oid = u64(oid)
        u_tid = tid and u64(tid)
        u_before_tid = before_tid and u64(before_tid)
        db = self.db
        if self.zodb_tid < (u_before_tid - 1 if before_tid else
                            u_tid or 0) <= self.zodb_ltid:
            o = None
        else:
            o = db.getObject(oid, tid, before_tid)
            if o and self.zodb_ltid < u64(o[0]) or \
               not self.inZodb(u_oid, u_tid, u_before_tid):
                return o
        p64 = util.p64
        zodb, z_oid = self.zodbFromOid(u_oid)
        try:
            value, serial, next_serial = zodb.loadBefore(p64(z_oid),
                before_tid or (util.p64(u_tid + 1) if tid else MAX_TID))
        except TypeError: # loadBefore returned None
            return False
        except POSKeyError:
            # loadBefore does not distinguish between an oid:
            # - that does not exist at any serial
            # - that was deleted
            # - whose creation was undone
            assert not o or o[3] is None, o
            return o
        if serial != tid:
            if tid:
                return False
            u_tid = u64(serial)
        if u_tid <= self.zodb_tid and o:
            return o
        value = zodb.repickle(value)
        if not next_serial:
            next_serial = db._getNextTID(db._getPartition(u_oid), u_oid, u_tid)
            if next_serial:
                next_serial = p64(next_serial)
        return (serial, next_serial, 0,
                util.makeChecksum(value),
                value,
                zodb.getDataTid(z_oid, u_tid))

    def getTransaction(self, tid, all=False):
        u64 = util.u64
        if self.zodb_tid < u64(tid) <= self.zodb_ltid:
            for zodb in self.zodb:
                for txn in zodb.iterator(tid, tid):
                    p64 = util.p64
                    shift_oid = zodb.shift_oid
                    return ([p64(u64(x.oid) + shift_oid) for x in txn],
                           ) + transactionAsTuple(txn)
        else:
            return self.db.getTransaction(tid, all)

    def getFinalTID(self, ttid):
        if util.u64(ttid) <= self.zodb_ltid and self._import:
            raise NotImplementedError
        return self.db.getFinalTID(ttid)

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        # Even if everything is imported, we can't truncate below
        # because it would import again if we restart with this backend.
        # This is also incompatible with writeback, because ZODB has
        # no API to truncate.
        if min_tid < self.zodb_ltid or self._writeback:
            # XXX: That's late to fail. The master should ask storage nodes
            #      whether truncation is possible before going further.
            raise NotImplementedError
        self.db._deleteRange(partition, min_tid, max_tid)

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        # This method is not tested and it is anyway
        # useless without getReplicationObjectList.
        raise BackendNotImplemented(self.getReplicationTIDList)
        p64 = util.p64
        tid = p64(self.zodb_tid)
        if min_tid <= tid:
            r = self.db.getReplicationTIDList(min_tid, min(max_tid, tid),
                                              length, partition)
            if max_tid <= tid:
                return r
            length -= len(r)
            min_tid = p64(self.zodb_tid + 1)
        else:
            r = []
        if length:
            tid = p64(self.zodb_ltid)
            if min_tid <= tid:
                u64 = util.u64
                def next_tid(i):
                    for txn in i:
                        tid = u64(txn.tid)
                        if self._getPartition(tid) == partition:
                            insort(z, (-tid, i))
                            break
                z = []
                for zodb in self.zodb:
                    next_tid(zodb.iterator(min_tid, min(max_tid, tid)))
                while z:
                    t, i = z.pop()
                    r.append(p64(-t))
                    length -= 1
                    if not length:
                        return r
                    next_tid(i)
            if tid < max_tid:
                r += self.db.getReplicationTIDList(max(min_tid, tid), max_tid,
                                                   length, partition)
        return r

    def _fetchObject(*_):
        raise AssertionError

    getLastObjectTID = Fallback.getLastObjectTID.__func__
    _getDataTID = Fallback._getDataTID.__func__

    def getObjectHistory(self, *args, **kw):
        raise BackendNotImplemented(self.getObjectHistory)

    def pack(self, *args, **kw):
        raise BackendNotImplemented(self.pack)


class WriteBack(object):

    _changed = False
    _process = None
    chunk_size = 100

    def __init__(self, db, storage):
        self._db = db
        self._storage = storage

    def close(self):
        if self._process:
            self._stop.set()
            self._event.set()
            self._process.join()

    def changed(self):
        self._changed = True

    def committed(self):
        if self._changed:
            self._changed = False
            if self._process:
                self._event.set()
            else:
                if FORK:
                    from multiprocessing import Process, Event
                else:
                    from threading import Thread as Process, Event
                self._event = Event()
                self._idle = Event()
                self._stop = Event()
                self._np = 1 + self._db._getMaxPartition()
                self._db = dumps(self._db, 2)
                self._process = Process(target=self._run)
                self._process.daemon = True
                self._process.start()

    @property
    def wait(self):
        # For unit tests.
        return self._idle.wait

    def _run(self):
        util.setproctitle('neostorage: write back')
        self._db = loads(self._db)
        try:
            @self._db.autoReconnect
            def _():
                # Unfortunately, copyTransactionsFrom does not abort in case
                # of failure, so we have to reopen.
                zodb = storageFromString(self._storage)
                try:
                    self.min_tid = util.add64(zodb.lastTransaction(), 1)
                    zodb.copyTransactionsFrom(self)
                finally:
                    zodb.close()
        finally:
            self._idle.set()
            self._db.close()

    def iterator(self):
        db = self._db
        np = self._np
        offset_list = xrange(np)
        while 1:
            with db:
                # Check the partition table at the beginning of every
                # transaction. Once the import is finished and at least one
                # cell is replicated, it is possible that some of this node
                # get outdated. In this case, wait for the next PT change.
                if np == len(db._readable_set):
                    while 1:
                        tid_list = []
                        max_tid = MAX_TID
                        for offset in offset_list:
                            x = db.getReplicationTIDList(
                                self.min_tid, max_tid, self.chunk_size, offset)
                            tid_list += x
                            if len(x) == self.chunk_size:
                                max_tid = x[-1]
                        if not tid_list:
                            break
                        tid_list.sort()
                        for tid in tid_list:
                            if self._stop.is_set():
                                return
                            yield TransactionRecord(db, tid)
                            if tid == max_tid:
                                break
                        else:
                            self.min_tid = util.add64(tid, 1)
                            break
                        self.min_tid = util.add64(tid, 1)
            if not self._event.is_set():
                self._idle.set()
                self._event.wait()
                self._idle.clear()
            self._event.clear()
            if self._stop.is_set():
                break


class TransactionRecord(BaseStorage.TransactionRecord):

    def __init__(self, db, tid):
        self._oid_list, user, desc, ext, _, _ = db.getTransaction(tid)
        super(TransactionRecord, self).__init__(tid, ' ', user, desc,
            loads(ext) if ext else {})
        self._db = db

    def __iter__(self):
        tid = self.tid
        for oid in self._oid_list:
            r = self._db.fetchObject(oid, tid)
            if r is None: # checkCurrentSerialInTransaction
                continue
            _, compression, _, data, data_tid = r
            if data is not None:
                data = compress.decompress_list[compression](data)
            yield BaseStorage.DataRecord(oid, tid, data, data_tid)
