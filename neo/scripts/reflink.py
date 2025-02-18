#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# reflink - track references between OIDs, external ZODB GC
#
# Copyright (C) 2025  Nexedi SA
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

"""
The main purpose of this tool is to perform a ZODB Garbage Collection
in a much more efficient way than the existing methods, which all compute
the whole graph of objects. Doing GC as part of IStorate.pack can't scale at
all and that would even very complex to implement in NEO. At the cost of
little extra storage space, this external GC is the best solution for NEO.

The core idea is to iterate over transactions and detect orphan OIDs by
analyzing changes between every pair of consecutives transactions: in a
directed graph of OIDs that is a tree, if A stops having a reference to B,
then B is orphan and the branch whose the root is B can be deleted. Therefore,
orphans can be identified by analyzing a very small part of the graph. We call
it "incremental GC" and it works in 2 phases:
- when iterating over transactions of the main DB, a list of possibly-orphan
  OIDs is computed (in the above example, it would only contain B, not the
  whole branch);
- the actual GC, which checks the list in case it contains OIDs that are
  actually not orphan, and extends it with branches.

(ZODB graphs are usually pure trees. This tool actually supports common
kinds of cycles as well as OIDs with several referrers. At worst, it would not
delete.)

This way, by tracking transactions as soon as they are committed, OIDs can be
deleted as soon as they're orphaned. This is actually important for an external
GC because a pack-without-GC can then keep some history while still cutting
after most deletes.

At last, a persistent ZODB is used to stores referrers & referents of each OID:
the whole process can be interrupted and resumed, and NEO can be chosen to
scale further.

This tool also implements what we call "full GC", which is the same as what
other GC do. It is so trivial to implement that there just can't be any bug
here. The incremental GC is quite complex and a full one can be used to check
whether any orphan was missed.

For the biggest DB, iterating over all transactions may take too much time.
An alternate method called "bootstrap" consists in starting from a recent
transaction but then it is not possible to run immediately incrementally:
- first, it scans all existing OIDs of the chosen transaction;
- then iterates over newer transactions without trying to identify orphans
  (it only updates the referrers/referents);
- and at last perform a full GC.
After this initial GC, it can switch to normal (i.e. incremental) operation.

This tool is called 'reflink' because it can have other uses that GC.
Since it stores the whole graph of OIDs with back-references, it can be used
to get instantaneously the referrers of an OID (and recursively its path from
the root OID), something that is missing in ZODB.

Internally, a rule is followed strictly: except for first phase of a bootstrap,
TIDs in the reflink DB match exactly those in the main DB. This is done to make
DB truncation as easy as possible, avoiding any conflict if the main DB has
consecutive TIDs (t0+1==t1). In other words, this tool never commits aside from
the analysis of a transaction of the main DB.

Warning: External GC is not compatible with the ZODB undo feature, because
there isn't any data GC transaction with transactions that orphaned the deleted
OIDs. IOW, if A loses its reference to B in t1, causing B to be orphan, and the
GC deletes B in t2, then nothing will prevent undoing t1 alone and A would have
a dangling reference. As a workaround, this tool can be run with a grace
period to prevent the GC from deleting too quickly: during this period, undoing
remains safe.

Warning: This tool does not track explicitly whether an oid exists or not,
Possible cases:
- Of course, an OID with referents implies that it exists.
- When bootstrapping, an existing OID always has an entry, possibly empty.
- During a normal operation, an OID without any referrer should have an entry
  by being part of the possibly-orphan list.
- An entry with only referrers mean nothing: the OID may exist or not.
  If considered orphan, GC tries to delete it and ignore if it's already
  non-existent.

TODO:
- Extend the NEO protocol with a mecanism to prevent packing at a TID that
  hasn't been analyzed yet by this tool (or reciprocally, to GC at a packed
  transaction).
- If needed, make the incremental GC handle any kind of graph by just checking
  connectivity of possibly-orphan OIDs with the root one (IOW, rewrite the
  Changeset.orphans method).
"""

from __future__ import print_function
import argparse, errno, logging, os, socket, sys, threading
from array import array
from bisect import insort
from collections import defaultdict
from contextlib import closing, contextmanager
from datetime import timedelta
from functools import partial
from io import BytesIO
from operator import attrgetter
from select import error as select_error, select
from time import gmtime, sleep, time
from Queue import Empty, Queue

from msgpack import dumps, loads
from pkg_resources import iter_entry_points
import ZODB
from persistent.TimeStamp import TimeStamp
from ZODB._compat import PersistentUnpickler
from ZODB.broken import Broken
from ZODB.POSException import ConflictError, POSKeyError
from ZODB.serialize import referencesf
from ZODB.utils import p64, u64, z64

TXN_GC_DESC = 'neoppod.git/reflink/gc@%x'
MAX_TXN_SIZE = 0x1fffff # MySQL limitation (MEDIUMBLOB)
VERSION = 1

logger = logging.getLogger('reflink')

array32u = partial(array, 'I')
assert array32u().itemsize == 4

try:
    from ZODB.Connection import TransactionMetaData
except ImportError: # BBB: ZODB < 5
    from ZODB.BaseStorage import TransactionRecord
    TransactionMetaData = lambda user='', description='', extension=None: \
        TransactionRecord(None, None, user, description, extension)

def checkAPI(storage):
    try:
        storage.app.oids.__call__
    except Exception:
        raise NotImplementedError(
            "Unable to iterate over all non-deleted OIDs of the"
            " reflink ZODB, which is required for a full GC (or a dump)."
            " Only NEO is known to have this non-standard API.")

def openStorage(uri):
    scheme = uri[:uri.find(':')]
    for ep in iter_entry_points('zodburi.resolvers'):
        if ep.name == scheme:
            factory, dbkw = ep.load()(uri)
            if dbkw:
                raise ValueError("Unexpected database options: %r" % dbkw)
            return factory()
    raise KeyError('No resolver found for uri: %s' % uri)

def inc64(tid):
    return p64(u64(tid) + 1)

def tidFromTime(t):
    return TimeStamp(*gmtime(t)[:5]+(t%60,)).raw()


class InvalidationListener(object):

    def __init__(self, storage, tid):
        self.last_gc = tid
        self._lock = l = threading.Lock()
        # Py3: use threading.Event (which has efficient timeout)
        self._new_pipe = os.pipe()
        self._new_flag = False
        storage.registerDB(self)
        self.invalidateCache = lambda: None
        ltid = storage.lastTransaction()
        del self.invalidateCache
        with l:
            if hasattr(self, 'last_tid'):
                return
            self.last_tid = ltid

    def invalidateCache(self):
        raise NotImplementedError

    def invalidate(self, transaction_id, oids, version=''):
        with self._lock:
            self.last_tid = transaction_id
            if not self._new_flag:
                self._new_flag = True
                os.write(self._new_pipe[1], '\0')

    def tpc_finish(self, tid):
        self.last_tid = self.last_gc = tid

    def wait(self, tid, timeout):
        with self._lock:
            if tid != self.last_tid:
                assert tid < self.last_tid, (tid, self.last_tid)
                return 0
            if self._new_flag:
                self._new_flag = False
                if os.read(self._new_pipe[0], 2) != '\0':
                    raise RuntimeError
        return self._wait(timeout)

    def _wait(self, timeout):
        t = time()
        try:
            if select(self._new_pipe[:1], (), (), timeout)[0]:
                # Let's give storages some time to release read locks
                # otherwise our reads would be significantly longer on average
                # and we'd commit more often (-i option), leading also to more
                # write amplification.
                # We should also try to not compete with other ZODB clients.
                sleep(1) # XXX: is it enough??
        except select_error as e:
            if e.args[0] != errno.EINTR:
                raise
        return time() - t

    transform_record_data = untransform_record_data = None


class OidArray(object):

    __slots__ = 'arrays',

    def __init__(self, arrays):
        a = self.arrays = defaultdict(array32u)
        for k, v in arrays:
            if v:
                a[k] = v

    def __iand__(self, other):
        arrays = self.arrays
        other = other.arrays
        for i in sorted(arrays):
            a = set(arrays[i])
            a.intersection_update(other.get(i, ()))
            if a:
                arrays[i] = array32u(sorted(a))
            else:
                del arrays[i]
        return self

    def __nonzero__(self):
        return any(self.arrays.itervalues())

    def __len__(self):
        return sum(map(len, self.arrays.itervalues()))

    def __iter__(self):
        arrays = self.arrays
        for i in sorted(arrays):
            x = arrays[i]
            i <<= 32
            for x in x:
                yield p64(i | x)

    def __delitem__(self, key):
        assert None is key.start is key.step, key
        n = key.stop
        arrays = self.arrays
        for i in sorted(arrays):
            x = arrays[i]
            k = len(x)
            if n < k:
                del x[:n]
                break
            n -= k
            del arrays[i]

    def append(self, oid):
        oid = u64(oid)
        self.arrays[oid >> 32].append(oid & 0xFFFFFFFF)


class Object(object):

    __slots__ = 'referrers', 'referents', 'prev_orphan', 'next_orphan'

    def __init__(self, oid, referrers=(), referents=(),
                 prev_orphan=None, next_orphan=None):
        self.referrers = [p64(oid + i) for i in referrers]
        self.referents = {p64(oid + i) for i in referents}
        self.prev_orphan = None if prev_orphan is None else \
            p64(oid + prev_orphan)
        self.next_orphan = None if next_orphan is None else \
            p64(oid + next_orphan)

    def __nonzero__(self):
        return bool(self.referrers or self.referents or
                    self.prev_orphan or self.next_orphan)

    def maybeOrphan(self):
        return self.referents.issuperset(self.referrers)

    def __repr__(self):
        return '<%s%s>' % (self.__class__.__name__, ' '.join(
            '%s=%r' % (attr, getattr(self, attr))
            for attr in self.__slots__))


class Changeset(object):

    def __init__(self, storage, bootstrap=None):
        self.storage = storage
        self._load = storage.load
        self.buckets = buckets = {}
        bucket = self._get(z64)
        if buckets[z64][0] is None:
            bucket['__reflink_version__'] = VERSION
            self._bootstrap = None
        else:
            version = bucket.get('__reflink_version__')
            if version != VERSION:
                raise Exception("unsupported reflink DB version %r"
                                " (expected %r)" % (version, VERSION))
            self._bootstrap = bucket.get('__reflink_bootstrap__')
            if bootstrap is None:
                buckets.clear()
            elif self._bootstrap is None or self._bootstrap[1] != 0:
                raise Exception("can not bootstrap: DB is not empty")
        if bootstrap is not None:
            checkAPI(storage)
            self.bootstrap = bootstrap, 0
        self._last_gc = bucket.get('__reflink_last_gc__', z64)
        self._last_pack = bucket.get('__reflink_last_pack__', z64)
        self._pack = None

    @partial(property, attrgetter('_last_gc'))
    def last_gc(self, value):
        self._last_gc = self._get(z64)['__reflink_last_gc__'] = value

    def pack(self, tid):
        self.storage.app.setPackOrder.__call__ # check non-standard API
        if self._last_pack < tid:
            self._pack = tid

    @partial(property, attrgetter('_bootstrap'))
    def bootstrap(self, value):
        self._bootstrap = value
        bucket = self._get(z64)
        if value is None:
            del bucket['__reflink_bootstrap__']
        else:
            bucket['__reflink_bootstrap__'] = value

    def abort(self):
        self.buckets.clear()

    def _get(self, oid):
        try:
            return self.buckets[oid][2]
        except KeyError:
            try:
                orig, serial = self._load(oid)
            except POSKeyError:
                bucket = {}
                self.buckets[oid] = None, z64, bucket
            else:
                bucket = loads(orig)
                self.buckets[oid] = orig, serial, bucket
        return bucket

    def get(self, oid):
        bucket = self._get(b'\0' + oid[:-1])
        key = ord(oid[-1]) # Py3
        obj = bucket.get(key, ())
        if not isinstance(obj, Object):
            obj = bucket[key] = Object(u64(oid), *obj)
        return obj

    def deleted(self, oid):
        bucket = self._get(b'\0' + oid[:-1])
        key = ord(oid[-1]) # Py3
        if not bucket.get(key, True):
            del bucket[key]

    def commit(self, tid, info=''):
        storage = self.storage
        now = time()
        logger.info('commit @ %x (%s)', u64(tid), info or
            'Δ %s' % timedelta(0, now - TimeStamp(tid).timeTime()))
        buckets = self.buckets
        txn = TransactionMetaData(extension={'time': now})
        storage.tpc_begin(txn, tid)
        def shortenVal(i):
            if i:
                return u64(i) - oid
        def shortenSeq(l):
            return tuple(u64(i) - oid for i in l)
        def shortenObj(x):
            if v[-1] == x:
                del v[-1]
                return True
        bootstrap = self._bootstrap
        if not bootstrap:
            pack = self._pack
            if pack:
                self._pack = None
                self._last_pack = self._get(z64)['__reflink_last_pack__'] = pack
                storage.app.setPackOrder(txn, pack)
        for bucket_oid, (orig, serial, bucket) in buckets.iteritems():
            base_oid = u64(bucket_oid) << 8
            data = {}
            for k, v in bucket.iteritems():
                if isinstance(v, Object):
                    oid = base_oid + k
                    v = [shortenSeq(v.referrers),
                         shortenSeq(sorted(v.referents)),
                         shortenVal(v.prev_orphan),
                         shortenVal(v.next_orphan)]
                    if (shortenObj(None) and shortenObj(None) and
                        shortenObj(()) and shortenObj(())):
                        if not bootstrap:
                            continue
                    v = tuple(v)
                data[k] = v
            if data:
                data = dumps(data)
                if orig != data:
                    storage.store(bucket_oid, serial, data, '', txn)
            elif orig:
                storage.deleteObject(bucket_oid, serial, txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)
        buckets.clear()

    def dump(self, tid):
        checkAPI(self.storage)
        buckets = self.buckets
        bootstrap = self._bootstrap
        assert not buckets
        try:
            if bootstrap is None:
                self._bootstrap = True
            with self.historical(tid):
                for oid in self.storage.app.oids(tid):
                    try:
                        bucket = self._get(oid)
                        oid = u64(oid) << 8
                        if not oid:
                            for x in sorted(x for x in bucket
                                              if type(x) is str):
                                print(x, repr(bucket.pop(x)))
                        for x in sorted(bucket):
                            x += oid
                            obj = self.get(p64(x))
                            print(x,
                                map(u64, obj.referrers),
                                map(u64, obj.referents),
                                obj.prev_orphan and u64(obj.prev_orphan),
                                obj.next_orphan and u64(obj.next_orphan))
                    finally:
                        buckets.clear()
        finally:
            self._bootstrap = bootstrap
            assert not buckets

    @contextmanager
    def historical(self, tid):
        if tid is None:
            yield
            return
        assert not self.buckets
        storage = self.storage
        load = partial(storage.loadBefore, tid=inc64(tid))
        try:
            self._load = lambda oid: load(oid)[:2]
            yield
        finally:
            self._load = storage.load
            self.abort()

    def full(self, tid):
        with self.historical(tid):
            import sqlite3
            with closing(sqlite3.connect(':memory:',
                                         check_same_thread=False)) as db:
                db.text_factory = bytes
                q = db.execute
                q('CREATE TABLE t (oid INTEGER PRIMARY KEY, referents BLOB)')
                count = 0
                for oid in self.storage.app.oids(tid):
                    x = u64(oid) << 8
                    for k, v in loads(self._load(oid)[0]).iteritems():
                        if isinstance(k, int):
                            q('INSERT INTO t VALUES(?,?)', (x+k,
                                dumps(v[1]) if len(v) > 1 and v[1] else None))
                            count += 1
                    db.commit()
                logger.info("  #oids: %s", count)
                stack = [0]
                x = xrange(256)
                while True:
                    for v in x:
                        try:
                            oid = stack.pop()
                        except IndexError:
                            (k, x), = q(
                                'SELECT MIN(oid) >> 32, MAX(oid) >> 32 FROM t')
                            return OidArray(() if k is None else ((k,
                                array32u(v for v, in q(
                                    'SELECT oid & 0xFFFFFFFF FROM t'
                                    ' WHERE oid>=? AND oid<? ORDER BY oid',
                                    (k << 32, k+1 << 32))))
                                for k in xrange(k, x + 1)))
                        k = oid,
                        try:
                            (v,), = q('SELECT referents FROM t WHERE oid=?', k)
                        except ValueError:
                            continue
                        q('DELETE FROM t WHERE oid=?', k)
                        if v:
                            stack += map(oid.__add__, loads(v))
                    db.commit()

    @property
    def orphan(self):
        buckets = self.buckets
        empty = not buckets
        root = self.get(z64)
        if empty:
            buckets.clear()
        return root.next_orphan

    def orphans(self, tid):
        # XXX: A heuristic is used for performance reasons. It would be wiser
        #      and maybe not significantly slower to always check connectivity
        #      with oid 0.
        with self.historical(tid):
            get = self.get
            orphans = set()
            check_orphan = set()
            check_cycle = []
            keep = {}
            obj = self.get(z64)
            if __debug__:
                seen = set()
            while True:
                oid = obj.next_orphan
                if not oid:
                    break
                if __debug__:
                    assert oid not in seen, u64(oid)
                    seen.add(oid)
                obj = get(oid)
                if obj.referrers:
                    check_cycle.append((oid, obj))
                else:
                    orphans.add(oid)
                    check_orphan |= obj.referents
            while check_orphan:
                oid = check_orphan.pop()
                obj = get(oid)
                if orphans.issuperset(obj.referrers):
                    orphans.add(oid)
                    check_orphan |= obj.referents
                else:
                    check_cycle.append((oid, obj))
            while check_cycle:
                oid, obj = check_cycle.pop()
                check_orphan = []
                cycle = {oid}
                x = obj
                while True:
                    referrers = set(x.referrers) - cycle - orphans
                    x = x.referents - cycle - orphans
                    if not referrers.issubset(x) or z64 in referrers:
                        # not a simple cycle, give up
                        if obj.prev_orphan:
                            keep[oid] = obj
                        break
                    check_orphan += x
                    cycle |= x
                    try:
                        x = get(check_orphan.pop())
                    except IndexError:
                        orphans |= cycle
                        break
            x = [u64(oid) for oid in keep if self.path(oid)[-1] != z64]
        if keep:
            if x:
                logger.error(
                    "It looks like the following oids can be"
                    " garbage-collected: %s. Please report.",
                    ','.join(map('%x'.__mod__, x)))
            for obj in keep.itervalues():
                self.keep(obj)
        return orphans

    def keep(self, obj):
        prev = obj.prev_orphan
        obj.prev_orphan = None
        oid = self.get(prev).next_orphan = obj.next_orphan
        if oid:
            obj.next_orphan = None
            self.get(oid).prev_orphan = prev

    def path(self, oid):
        orphan = []
        path = []
        stack = []
        x = iter((oid,))
        while True:
            try:
                oid = next(x)
            except StopIteration:
                if len(orphan) < len(path):
                    orphan = path[:]
                try:
                    x = stack.pop()
                except IndexError:
                    return orphan
                del path[-1]
            else:
                if oid in path:
                    continue
                path.append(oid)
                if oid == z64:
                    return path
                stack.append(x)
                x = iter(self.get(oid).referrers)


class ArgumentDefaultsHelpFormatter(argparse.HelpFormatter):

    def _format_action(self, action):
        if action.const is None is not action.default is not argparse.SUPPRESS:
            default = '(default: %(default)s)'
            help = action.help
            if help:
                if not help.endswith(default):
                    action.help = help + ' ' + default
            else:
                action.help = default
        return super(ArgumentDefaultsHelpFormatter, self)._format_action(action)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    kw = dict(formatter_class=ArgumentDefaultsHelpFormatter)
    parser = argparse.ArgumentParser(description=
        "External ZODB GC by tracking references between OIDs and marking"
        " orphaned objects as deleted automatically. Garbage Collection only"
        " marks objects as non-existent and data is actually deleted when the"
        " DB is packed. GC can't work correctly at a revision older than a"
        " pack so make sure to pack at a revision that is already processed:"
        " there's currently no mechanism to avoid such mistake.",
        **kw)
    _ = parser.add_argument
    _('-v', '--verbose', action='store_true',
        help="Verbose mode, display more information.")
    _('refs', metavar="REFS_URI", help="ZODB containing tracking data."
        " If the main DB is truncated, then this DB must be truncated at the"
        " same TID. Packing this DB does not need any GC.")
    parsers = parser.add_subparsers(dest='command')

    def gc(jobs_help):
        _ = s.add_argument
        _('-f', '--full', action='store_true',
            help="Always perform a full GC, instead of an incremental GC."
                 " This should only be used occasionally, preferably with -n"
                 " to only check if an incremental GC is enough: the later"
                 " uses a heuristic in case of cycles in the graph of oids"
                 " and it should cover real use cases.")
        _('-j', '--jobs', type=int, metavar="N", default=1, help=jobs_help)
        _('-m', '--max-txn-size', type=int, metavar="N", default=MAX_TXN_SIZE,
            help='Maximum number of OIDs to delete per transaction.')
        _('-n', '--dry-run', metavar="DUMP",
            help="Save OIDs that would be deleted during a GC into the"
                 " specified file and exit. ERP5 uid is given in parentheses"
                 " if is exists. Both oid & uid are output in hexadecimal.")
        _('main', metavar="MAIN_URI", help="ZODB to track.")
    def period(default=0, extra_help=""):
        s.add_argument(
            '-p', '--period', type=float, metavar="SECONDS", default=default,
            help="Age of the historical revision at which a GC is performed"
                 " (this can be seen as a grace period).%s This is mainly to"
                 " work around possible conflicts with undo: an orphan OID may"
                 " be referenced again by an undone transactions, requiring to"
                 " also undo GC transactions that happened after. So when this"
                 " argument is not 0, a GC calculates twice the list of OIDs to"
                 " delete, now and in the past, to exclude those that should be"
                 " kept." % extra_help)
    tid_arg = dict(metavar="TID", type=eval, help="TID as Python integer.")

    _ = parsers.add_parser('bootstrap',
        help="By default, all transactions since the creation of main DB are"
             " scanned. In the case where such history would take too long to"
             " process, you can try this command so that the 'run' command"
             " starts with 2 extra steps: scan the whole main DB at the given"
             " TID and do a full GC. Both DB must provide a non-standard API"
             " that only NEO is known to have.",
        **kw).add_argument
    _('tid', **tid_arg)

    _ = parsers.add_parser('dump',
        help="Dump the whole reflink DB to stdout.",
        **kw).add_argument
    _('tid', nargs='?', **tid_arg)

    s = parsers.add_parser('gc',
        help="Do a GC immediately, ignoring new transactions, and exit.",
        **kw)
    gc("Concurrent reads from main DB during GC.")
    period()

    _ = parsers.add_parser('path',
        help="Get ancestors of an OID.",
        **kw).add_argument
    _('--tid', **tid_arg)
    _('oid', metavar="OID", type=eval,
        help="OID as Python integer.")
    _('main', metavar="MAIN_URI", nargs='?',
        help="Tracked ZODB, for class information.")

    s = parsers.add_parser('run',
        help="Track references and GC automatically. This is the main command."
             " GCs are performed when there's no new transaction to process.",
             **kw)
    gc("Concurrent reads from main DB during bootstrap & GC.")
    _ = s.add_mutually_exclusive_group().add_argument
    _('-0', '--exit-before-gc', action='store_true',
        help="Exit before a GC."
             " This option must be used when tracking a NEO DB in backup mode.")
    _('-1', '--exit-after-gc', action='store_true',
        help="Exit after a GC.")
    _('-N', '--no-gc', action='store_true',
        help="Only track references.")
    _ = s.add_argument
    _('-i', '--commit-interval', type=float, metavar="SECONDS", default=10,
        help="Commit every SECONDS of work.")
    _('--pack-neo', type=float, metavar="EPOCH",
        help="Pack time in seconds since the epoch. This argument is ignored"
             " during bootstrap and it is only to pack the refs DB when it is"
             " run by NEO. Other IStorage implementations don't store pack"
             " commands in transactions and pack() can be used as long as it's"
             " done without GC.")
    period(86400,
        " For performance reasons, this revision won't be older than the"
        " previous GC commit so GCs may be delayed this number of seconds.")

    args = parser.parse_args(args)

    global logging_handler
    try:
        logging_handler
    except NameError:
        logging_handler = logging.StreamHandler()
        logging_handler.setFormatter(logging.Formatter(
            '%(asctime)s.%(msecs)03u %(levelname)-9s %(message)s',
            '%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(logging_handler)
    if args.verbose:
        logger.setLevel(logging.INFO)

    command = args.command
    bootstrap = command == "bootstrap"
    changeset = Changeset(openStorage(args.refs),
                          args.tid if bootstrap else None)

    if command == "dump":
        tid = args.tid
        changeset.dump(None if tid is None else p64(tid))
        return

    tid = changeset.storage.lastTransaction()

    if bootstrap:
        changeset.commit(inc64(tid))
        print("Bootstrap at %s UTC. You can now use the 'run' command."
              % TimeStamp(p64(args.tid)))
        return

    if args.main:
        main_storage = openStorage(args.main)
        for iface in (ZODB.interfaces.IStorageIteration,
                      ZODB.interfaces.IExternalGC):
            if not iface.providedBy(main_storage):
                return "Main storage shall implement " + iface.__name__

    if command == "path":
        x = args.tid
        if x is None:
            tid = inc64(tid)
        else:
            tid = p64(x + 1)
            x = p64(x)
        def find_global(*args):
            obj.extend(args)
        with changeset.historical(x):
            for oid in reversed(changeset.path(p64(args.oid))):
                obj = [hex(u64(oid))]
                if args.main:
                    data = main_storage.loadBefore(oid, tid)[0]
                    PersistentUnpickler(find_global, None,
                                        BytesIO(data)).load()
                print(*obj)
        return

    bootstrap = changeset.bootstrap
    if command == 'gc':
        if bootstrap and not (bootstrap[1] is None and args.dry_run):
            return "can not GC: bootstrapping"
        commit_interval = 0
        exit_before_gc = no_gc = False
        exit_after_gc = True
    else:
        assert command == "run", command
        commit_interval = args.commit_interval
        if commit_interval <= 0:
            parser.error("--commit-interval must be strictly positive.")
        exit_before_gc = args.exit_before_gc
        exit_after_gc = args.exit_after_gc
        no_gc = args.no_gc
        x = args.pack_neo
        if x:
            changeset.pack(tidFromTime(x))
    job_count = args.jobs
    if job_count <= 0:
        parser.error("--jobs must be strictly positive.")
    max_txn_size = args.max_txn_size
    if not 0 < max_txn_size <= MAX_TXN_SIZE:
        parser.error("--max_txn_size value not in [1..%s]." % MAX_TXN_SIZE)
    period = args.period
    if period < 0:
        parser.error("--period must be positive.")

    full = args.full
    if full:
        checkAPI(changeset.storage)

    dry_run = args.dry_run
    if dry_run:
        dry_run_stats = defaultdict(list)
        def find_global(*args):
            dry_run_stats[args].append(oid)
        uid_dict = {}
        load_broken = lambda *args: Broken
        load_persistent = lambda *args: None

    del args, command, parser

    queue = Queue(3)
    exc_info = [] # Py3
    def checkExc():
        if exc_info:
            etype, value, tb = exc_info
            raise etype, value, tb

    if bootstrap and bootstrap[1] is not None:
        @apply
        def next_oid():
            tid, oid = map(p64, bootstrap)
            load = partial(main_storage.loadBefore, tid=inc64(tid))
            iter_oids = main_storage.app.oids(tid, oid)
            if job_count == 1:
                def loadThread():
                    put = queue.put
                    try:
                        for oid in iter_oids:
                            put((oid, load(oid)[0]))
                    except:
                        exc_info[:] = sys.exc_info()
                    put(None)
                t = threading.Thread(target=loadThread)
                t.daemon = True
                t.start()
                return queue.get
            job_queue = Queue(2 * job_count)
            Lock = threading.Lock
            def loadThread(iter_lock=Lock(), put=job_queue.put):
                try:
                    while True:
                        l = Lock()
                        with l:
                            with iter_lock:
                                if exc_info:
                                    return
                                oid = next(iter_oids)
                                r = [oid, None, l]
                                put(r)
                            r[1] = load(oid)[0]
                except StopIteration:
                    pass
                except:
                    exc_info[:] = sys.exc_info()
                put(None)
            for t in xrange(job_count):
                t = threading.Thread(target=loadThread)
                t.daemon = True
                t.start()
            def next_oid(get=job_queue.get):
                r = get()
                if r:
                    r[2].acquire()
                    return r[:2]
            return next_oid
        next_commit = time() + commit_interval
        while True:
            r = next_oid()
            if r is None:
                checkExc()
                break
            oid, data = r
            try:
                referents = set(referencesf(data))
            except Exception:
                logger.warning("Corrupted record %x@%x", u64(oid), bootstrap[0])
                referents = set()
            else:
                referents.discard(oid)
            src = changeset.get(oid)
            src.referents = referents
            for referent in referents:
                insort(changeset.get(referent).referrers, oid)
            x = time()
            if next_commit <= x:
                tid = inc64(tid)
                changeset.bootstrap = bootstrap[0], u64(oid) + 1
                changeset.commit(tid, "oid=%x" % u64(oid))
                next_commit = x + commit_interval
        changeset.bootstrap = bootstrap[0], None
        tid = p64(bootstrap[0])
        changeset.commit(tid)
        del next_oid

    if job_count == 1:
        def iter_orphans(iter_oids, load):
            for i, oid in iter_oids:
                try:
                    data, serial = load(oid)
                except POSKeyError:
                    data = serial = None
                yield i, oid, data, serial
    else:
        def iter_orphans(iter_oids, load):
            job_queue = Queue(2 * job_count)
            Lock = threading.Lock
            def loadThread(iter_lock=Lock(),
                           put=job_queue.put):
                try:
                    while True:
                        l = Lock()
                        with l:
                            with iter_lock:
                                if exc_info:
                                    return
                                i, oid = next(iter_oids)
                                r = [l, i, oid]
                                put(r)
                            try:
                                r += load(oid)
                            except POSKeyError:
                                r += None, None
                except StopIteration:
                    pass
                except:
                    exc_info[:] = sys.exc_info()
                put(None)
            threads = []
            try:
                for t in xrange(job_count):
                    t = threading.Thread(target=loadThread)
                    t.daemon = True
                    t.start()
                    threads.append(t)
                get = job_queue.get
                while True:
                    r = get()
                    if r is None:
                        break
                    r[0].acquire()
                    yield r[1:]
            finally:
                iter_oids = iter(())
                try:
                    while True:
                        get(False)
                except Empty:
                    pass
                for t in threads:
                    t.join()
            checkExc()

    invalidation_listener = InvalidationListener(main_storage, tid)

    if commit_interval:
        deleted_dict = {}
        def iterTrans(x):
            put = queue.put
            try:
                for x in x:
                    tid = x.tid
                    put(tid)
                    try:
                        x = deleted_dict.pop(tid)
                    except KeyError:
                        for x in x:
                            put((x.oid, x.data))
                    else:
                        for x in x:
                            put((x, None))
                    put(None)
            except:
                exc_info[:] = sys.exc_info()
            put(None)

    if not no_gc:
        gc_lock_name = "\0reflink-%s" % os.getpid()
    next_gc = period and TimeStamp(changeset.last_gc).timeTime() + period
    next_commit = time() + commit_interval
    while True:
        if commit_interval:
            thread = threading.Thread(target=iterTrans,
                args=(main_storage.iterator(inc64(tid)),))
            thread.daemon = True
            thread.start()
            while True:
                x = queue.get()
                if x is None:
                    checkExc()
                    break
                tid = x
                # logger.debug("tid=%x", u64(tid))
                check_orphan = {}
                check_keep = set()
                check_deleted = []
                while True:
                    x = queue.get()
                    if x is None:
                        checkExc()
                        break
                    oid, data = x
                    # logger.debug("  oid=%x", u64(oid))
                    src = changeset.get(oid)
                    old_set = src.referents
                    if data is None:
                        # logger.debug("    deleted")
                        if src.prev_orphan:
                            changeset.keep(src)
                        elif bootstrap:
                            check_deleted.append(oid)
                        new_set = set()
                    else:
                        if not (src.prev_orphan or old_set or src.referrers
                                or oid == z64 or bootstrap):
                            # After a pack, an oid may be
                            # orphan from its creation.
                            check_orphan[oid] = src
                        try:
                            new_set = set(referencesf(data))
                        except Exception:
                            logger.warning("Corrupted record %x@%x",
                                           u64(oid), u64(tid))
                            new_set = set()
                        else:
                            new_set.discard(oid)
                    if new_set == old_set:
                        continue
                    src.referents = new_set
                    for referent in new_set - old_set:
                        # logger.debug("  + %x", u64(referent))
                        dst = changeset.get(referent)
                        insort(dst.referrers, oid)
                        if dst.prev_orphan:
                            check_keep.add(dst)
                    old_set.difference_update(new_set)
                    for referent in old_set:
                        # logger.debug("  - %x", u64(referent))
                        dst = changeset.get(referent)
                        dst.referrers.remove(oid)
                        if not (dst.prev_orphan
                                or referent == z64 or bootstrap):
                            check_orphan[referent] = dst
                x = inc64(tid)
                for obj in check_keep:
                    if not obj.maybeOrphan():
                        changeset.keep(obj)
                for oid, obj in check_orphan.iteritems():
                    if obj.maybeOrphan():
                        assert not (obj.prev_orphan or obj.next_orphan), oid
                        if not obj.referents:
                            try:
                                if main_storage.loadBefore(oid, x) is None:
                                    continue
                            except POSKeyError:
                                continue
                        obj.prev_orphan = z64
                        prev_orphan = changeset.get(z64)
                        next_orphan = prev_orphan.next_orphan
                        if next_orphan:
                            changeset.get(next_orphan).prev_orphan = oid
                            obj.next_orphan = next_orphan
                        prev_orphan.next_orphan = oid
                for oid in check_deleted:
                    changeset.deleted(oid)
                del check_keep, check_deleted, check_orphan

                x = time()
                if next_commit <= x:
                    changeset.commit(tid)
                    next_commit = x + commit_interval
            thread.join()
            assert not deleted_dict, list(deleted_dict)

        x = time()
        timeout = next_gc - x
        if timeout <= 0 or not commit_interval:
            timeout = None
            if (full or changeset.orphan or bootstrap) and not no_gc:
                if changeset.buckets:
                    next_commit = x + commit_interval
                    changeset.commit(tid)
                assert tid == changeset.storage.lastTransaction()
                if exit_before_gc:
                    break
                if full or bootstrap:
                    logger.info('Full GC...')
                    gc = changeset.full
                else:
                    logger.info('Incremental GC...')
                    gc = changeset.orphans
                gc_tid = tid if bootstrap or not period else \
                    tidFromTime(time() - period)
                if gc_tid < tid:
                    if gc_tid < changeset.last_gc:
                        logger.warning(
                            "Doing GC at a TID (0x%x) older than"
                            " the previous one (0x%x)",
                            u64(gc_tid), u64(changeset.last_gc))
                    orphans = gc(gc_tid)
                    if orphans:
                        changeset.abort()
                        x = gc(None)
                        orphans &= x
                        if x and not orphans:
                            next_gc = TimeStamp(tid).timeTime() + period
                            timeout = max(0, next_gc - time())
                        del x
                else:
                    gc_tid = tid
                    orphans = gc(None)
                if isinstance(orphans, set):
                    orphans = sorted(orphans)
                logger.info('  found %s OID(s) to delete', len(orphans))
                log_remaining = False
                count = 0
                while orphans:
                    if log_remaining:
                        logger.info('  %s remaining...', len(orphans))
                    else:
                        log_remaining = True
                    with closing(socket.socket(socket.AF_UNIX,
                                               socket.SOCK_STREAM)) as s:
                        try:
                            s.connect(gc_lock_name)
                            s.recv(1)
                        except socket.error:
                            pass
                    deleted = OidArray([]) # only used to speed up
                    txn = TransactionMetaData(
                        description=TXN_GC_DESC % u64(gc_tid))
                    main_storage.tpc_begin(txn)
                    try:
                        for i, oid, data, serial in iter_orphans(
                                enumerate(orphans, 1), main_storage.load):
                            if serial is None:
                                if full:
                                    changeset.deleted(oid)
                                continue
                            if gc_tid < serial:
                                count = None
                                break
                            main_storage.deleteObject(oid, serial, txn)
                            deleted.append(oid)
                            if dry_run:
                                oid = u64(oid)
                                try:
                                    x = PersistentUnpickler(find_global,
                                        load_persistent, BytesIO(data))
                                    x.load()
                                except Exception:
                                    assert data is not None, oid
                                    dry_run_stats[
                                        ('corrupted',) if data else ('empty',)
                                        ].append(oid)
                                else:
                                    x.find_global = load_broken
                                    try:
                                        uid_dict[oid] = x.load()['uid']
                                    except Exception:
                                        pass
                            count += 1
                            if count == max_txn_size:
                                del orphans[:i]
                                break
                        else:
                            orphans = None
                        if count:
                            logger.info('  tpc_vote (%s delete(s))...', count)
                            main_storage.tpc_vote(txn)
                            if dry_run:
                                count = None
                                with os.fdopen(os.open(dry_run,
                                        os.O_WRONLY | os.O_CREAT | os.O_EXCL, # Py3
                                        0o666), 'w') as f:
                                    for x in sorted(dry_run_stats):
                                        y = []
                                        for oid in dry_run_stats.pop(x):
                                            uid = uid_dict.get(oid)
                                            y.append(
                                                '%x' % oid if uid is None else
                                                '%x(%x)' % (oid, uid))
                                        print(len(y), '.'.join(x), *y,
                                              sep=',', file=f)
                                return
                            logger.info('  tpc_finish...')
                            main_storage.tpc_finish(txn,
                                invalidation_listener.tpc_finish)
                            x = changeset.last_gc = \
                                invalidation_listener.last_gc
                            if commit_interval:
                                deleted_dict[x] = deleted
                            next_commit = 0
                            if period:
                                # We don't want future `gc(gc_tid)` to waste
                                # time with OIDs that are already deleted.
                                next_gc = TimeStamp(x).timeTime() + period
                            continue
                    except ConflictError:
                        count = None
                    finally:
                        if count:
                            count = 0
                        else:
                            main_storage.tpc_abort(txn)
                        del deleted
                    break
                del orphans
                if count == 0:
                    if bootstrap:
                        bootstrap = changeset.bootstrap = None
                        logger.info("Bootstrap completed")
                        next_commit = 0
                    if exit_after_gc:
                        break
            if not commit_interval:
                break

        next_commit += invalidation_listener.wait(tid, timeout)


if __name__ == '__main__':
    sys.exit(main())
