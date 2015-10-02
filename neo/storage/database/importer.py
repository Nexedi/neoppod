#
# Copyright (C) 2014-2015  Nexedi SA
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
import cPickle, pickle, time
from bisect import bisect, insort
from collections import deque
from cStringIO import StringIO
from ConfigParser import SafeConfigParser
from ZODB.config import storageFromString
from ZODB.POSException import POSKeyError

from . import buildDatabaseManager, DatabaseManager
from neo.lib import logging, patch, util
from neo.lib.exception import DatabaseFailure
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID, ZERO_HASH, MAX_TID

patch.speedupFileStorageTxnLookup()

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
        # For pickling, it is possible to use the fastest implementation,
        # which also generates fewer useless PUT opcodes.
        self._p = cPickle.Pickler(self._f, 1)
        self.memo = self._p.memo # just a tiny optimization

        def persistent_id(obj):
            if isinstance(obj, Reference):
                r = obj.value
                del obj.value # minimize refcnt like for deque+popleft
                return r
        self._p.inst_persistent_id = persistent_id

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
            try:
                self._p.dump(classmeta).dump(state)
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

    del dispatch[pickle.NEWOBJ] # ZODB has never used protocol 2

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
        del state["data_tid"], state["storage"]
        return state

    def connect(self, storage):
        self.data_tid = {}
        self.storage = storageFromString(storage)

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
            elif isinstance(obj, str):
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
            return oid if isinstance(obj, str) else (oid, obj[1])
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

    def __init__(self, zodb, *args, **kw):
        iterator = zodb.iterator(*args, **kw)
        def _next():
            self.transaction = next(iterator)
        _next()
        self.zodb = zodb
        self.next = _next

    tid = property(lambda self: self.transaction.tid)

    def __lt__(self, other):
        return self.tid < other.tid or self.tid == other.tid \
            and self.zodb.shift_oid < other.zodb.shift_oid


class ImporterDatabaseManager(DatabaseManager):
    """Proxy that transparently imports data from a ZODB storage
    """
    _last_commit = 0

    def __init__(self, *args, **kw):
        super(ImporterDatabaseManager, self).__init__(*args, **kw)
        self.db._connect()

    _uncommitted_data = property(
        lambda self: self.db._uncommitted_data,
        lambda self, value: setattr(self.db, "_uncommitted_data", value))

    def _parse(self, database):
        config = SafeConfigParser()
        config.read(os.path.expanduser(database))
        sections = config.sections()
        # XXX: defaults copy & pasted from elsewhere - refactoring needed
        main = {'adapter': 'MySQL', 'wait': 0}
        main.update(config.items(sections.pop(0)))
        self.zodb = ((x, dict(config.items(x))) for x in sections)
        self.compress = main.get('compress', 1)
        self.db = buildDatabaseManager(main['adapter'],
            (main['database'], main.get('engine'), main['wait']))
        for x in """query erase getConfiguration _setConfiguration
                    getPartitionTable changePartitionTable getUnfinishedTIDList
                    dropUnfinishedData storeTransaction finishTransaction
                    storeData
                 """.split():
            setattr(self, x, getattr(self.db, x))

    def commit(self):
        self.db.commit()
        self._last_commit = time.time()

    def setNumPartitions(self, num_partitions):
        self.db.setNumPartitions(num_partitions)
        try:
            del self._getPartition
        except AttributeError:
            pass

    def close(self):
        self.db.close()
        if isinstance(self.zodb, list): # _setup called
            for zodb in self.zodb:
                zodb.close()

    def _setup(self):
        self.db._setup()
        zodb_state = self.getConfiguration("zodb")
        if zodb_state:
            logging.warning("Ignoring configuration file for oid mapping."
                            " Reloading it from NEO storage.")
            zodb = cPickle.loads(zodb_state)
            for k, v in self.zodb:
                zodb[k].connect(v["storage"])
        else:
            zodb = {k: ZODB(**v) for k, v in self.zodb}
            x, = (x for x in zodb.itervalues() if not x.oid)
            x.setup(zodb)
            self.setConfiguration("zodb", cPickle.dumps(zodb))
        self.zodb_index, self.zodb = zip(*sorted(
            (x.shift_oid, x) for x in zodb.itervalues()))
        self.zodb_ltid = max(x.ltid for x in self.zodb)
        zodb = self.zodb[-1]
        self.zodb_loid = zodb.shift_oid + zodb.next_oid - 1
        self.zodb_tid = self.db.getLastTID(self.zodb_ltid) or 0
        self._import = self._import()

    def doOperation(self, app):
        if self._import:
            app.newTask(self._import)

    def _import(self):
        p64 = util.p64
        u64 = util.u64
        tid = p64(self.zodb_tid + 1)
        zodb_list = []
        for zodb in self.zodb:
            try:
                zodb_list.append(ZODBIterator(zodb, tid, p64(self.zodb_ltid)))
            except StopIteration:
                pass
        tid = None
        def finish():
            if tid:
                self.storeTransaction(tid, object_list, (
                    (x[0] for x in object_list),
                    str(txn.user), str(txn.description),
                    cPickle.dumps(txn.extension), False, tid), False)
                self.releaseData(data_id_list)
                logging.debug("TXN %s imported (user=%r, desc=%r, len(oid)=%s)",
                    util.dump(tid), txn.user, txn.description, len(object_list))
                del object_list[:], data_id_list[:]
                if self._last_commit + 1 < time.time():
                    self.commit()
                self.zodb_tid = u64(tid)
        if self.compress:
            from zlib import compress
        else:
            compress = None
            compression = 0
        object_list = []
        data_id_list = []
        while zodb_list:
            zodb_list.sort()
            z = zodb_list[0]
            # Merge transactions with same tid. Only
            # user/desc/ext from first ZODB are kept.
            if tid != z.tid:
                finish()
                txn = z.transaction
                tid = txn.tid
                yield 1
            zodb = z.zodb
            for r in z.transaction:
                oid = p64(u64(r.oid) + zodb.shift_oid)
                data_tid = r.data_txn
                if data_tid or r.data is None:
                    data_id = None
                else:
                    data = zodb.repickle(r.data)
                    if compress:
                        compressed_data = compress(data)
                        compression = len(compressed_data) < len(data)
                        if compression:
                            data = compressed_data
                    checksum = util.makeChecksum(data)
                    data_id = self.holdData(util.makeChecksum(data), data,
                                            compression)
                    data_id_list.append(data_id)
                object_list.append((oid, data_id, data_tid))
                # Give the main loop the opportunity to process requests
                # from other nodes. In particular, clients may commit. If the
                # storage node exits after such commit, and before we actually
                # update 'obj' with 'object_list', some rows in 'data' may be
                # unreferenced. This is not a problem because the leak is
                # solved when resuming the migration.
                yield 1
            try:
                z.next()
            except StopIteration:
                del zodb_list[0]
        self._last_commit = 0
        finish()
        logging.warning("All data are imported. You should change"
            " your configuration to use the native backend and restart.")
        self._import = None
        for x in """getObject objectPresent getReplicationTIDList
                 """.split():
            setattr(self, x, getattr(self.db, x))

    def inZodb(self, oid, tid=None, before_tid=None):
        return oid <= self.zodb_loid and (
            self.zodb_tid < before_tid if before_tid else
            tid is None or self.zodb_tid < tid <= self.zodb_ltid)

    def zodbFromOid(self, oid):
        zodb = self.zodb[bisect(self.zodb_index, oid) - 1]
        return zodb, oid - zodb.shift_oid

    def getLastIDs(self, all=True):
        tid, _, _, oid = self.db.getLastIDs(all)
        return (max(tid, util.p64(self.zodb_ltid)), None, None,
                max(oid, util.p64(self.zodb_loid)))

    def objectPresent(self, oid, tid, all=True):
        r = self.db.objectPresent(oid, tid, all)
        if not r:
            u_oid = util.u64(oid)
            u_tid = util.u64(tid)
            if self.inZodb(u_oid, u_tid):
                zodb, oid = self.zodbFromOid(u_oid)
                try:
                    return zodb.loadSerial(util.p64(oid), tid)
                except POSKeyError:
                    pass

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
            assert not o, o
            return o
        if serial != tid:
            if tid:
                return False
            u_tid = u64(serial)
        if u_tid <= self.zodb_tid and o:
            return o
        if value:
            value = zodb.repickle(value)
            checksum = util.makeChecksum(value)
        else:
            # CAVEAT: Although we think loadBefore should not return an empty
            #         value for a deleted object (see comment in NEO Storage),
            #         there's no need to distinguish this case in the above
            #         except clause because it would be crazy to import a
            #         NEO DB using this backend.
            checksum = None
        return (serial, next_serial or
            db._getNextTID(db._getPartition(u_oid), u_oid, u_tid),
            0, checksum, value, zodb.getDataTid(z_oid, u_tid))

    def getTransaction(self, tid, all=False):
        u64 = util.u64
        if self.zodb_tid < u64(tid) <= self.zodb_ltid:
            for zodb in self.zodb:
                for txn in zodb.iterator(tid, tid):
                    p64 = util.p64
                    shift_oid = zodb.shift_oid
                    return ([p64(u64(x.oid) + shift_oid) for x in txn],
                        txn.user, txn.description,
                        cPickle.dumps(txn.extension), 0, tid)
        else:
            return self.db.getTransaction(tid, all)

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
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

