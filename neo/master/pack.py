#
# Copyright (C) 2020  Nexedi SA
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

# IDEA: Keep minimal information to avoid useless memory usage, e.g. with
#       arbitrary data large like a list of OIDs. Only {tid: id} is important:
#       everything could be queried from storage nodes when needed. Note
#       however that extra information allows the master to automatically drop
#       redundant pack orders: keeping partial/time may be an acceptable cost.

from functools import partial
from operator import attrgetter
from weakref import proxy
from neo.lib.connection import ConnectionClosed
from neo.lib.protocol import Packets


class Pack(object):

    def __init__(self, tid, id, partial, oids, time):
        self.tid = tid
        self.id = id
        self.partial = partial
        self.oids = oids
        self.time = time
        self._waiting = []

    @property
    def waitForPack(self):
        return self._waiting.append

    def completed(self):
        for callback in self._waiting:
            callback()
        del self._waiting

    def connectionLost(self, conn):
        try:
            self._waiting.remove(conn)
        except ValueError:
            pass


class RequestOld(object):

    caller = None

    def __init__(self, app, pack_id, limit, caller):
        self.app = proxy(app)
        self.caller = caller
        self.pack_id = pack_id
        self.limit = limit
        self.offsets = set(xrange(app.pt.getPartitions()))
        self.packs = []
        # In the case PT changes, we may ask a node again before it replies
        # to previous requests, so we can't simply use its id as key.
        self.querying = set()
        app.pm.old.append(self)
        self._ask()

    def connectionLost(self, conn):
        if self.caller != conn:
            nid = conn.getUUID()
            x = [x for x in self.querying if x[0] == nid]
            if x:
                self.querying.difference_update(x)
                self._ask()
            return True
        self.__dict__.clear()

    def _ask(self):
        getCellList = self.app.pt.getCellList
        readable = defaultdict(list)
        for offset in self.offsets:
            for cell in getCellList(offset, True):
                readable[cell.getUUID()].append(offset)
        offsets = self.offsets.copy()
        for x in self.querying:
            offsets.difference_update(x[1])
        # We ask more than self.limit else we can't keep results in PackManager
        # without being sure there's no hole.
        p = Packets.AskPackOrders(self.pack_id, None)
        while offsets:
            node = getCellList(offsets.pop(), True)[0].getNode()
            nid = node.getUUID()
            x = tuple(readable.pop(nid))
            offsets.difference_update(x)
            x = nid, x
            self.querying.add(x)
            node.ask(p, process=partial(self._answer, x))

    def _answer(self, nid_offsets, pack_list):
        if self.caller:
            self.querying.remove(nid_offsets)
            self.offsets.difference_update(nid_offsets[1])
            self.packs += pack_list
            if self.offsets:
                self._ask()
            else:
                app = self.app
                pm = app.pm
                for pack_order in pack_list:
                    pm.add(*pack_order)
                self.caller([
                    (p.tid, p.id, p.partial, p.oids, p.time)
                    for p in islice(pm.sorted(self.pack_id), self.limit)])
                pm.notifyCompleted(app.getCompletedPackId())


class PackManager(object):

    def __init__(self):
        self.last_id = 0
        self.packs = {}
        self.old = []

    reset = __init__

    def add(self, tid, *args):
        p = self.packs.get(tid)
        if p is None:
            p = self.packs[tid] = Pack(tid, *args)
        elif p.id is None:
            p.id = args[0]
        if None is not p.id > self.last_id:
            self.last_id = p.id

    @apply
    def sorted():
        by_tid = attrgetter('tid')
        by_id = attrgetter('id')
        return lambda self, pack_id: sorted(
                (p for p in self.packs.itervalues() if p.id is None),
                key=by_tid,
            ) if pack_id is None else sorted(
                (p for p in self.packs.itervalues()
                    if None is not p.id >= pack_id),
                key=by_id,
            )

    def new(self, tid, oids, time):
        i = self.last_id = 1 + self.last_id
        p = self.packs[tid] = Pack(tid, i, bool(oids), oids, time)
        return i

    def getMinPackId(self):
        packs = self.packs
        if packs:
            return min(p.id for p in packs.itervalues())

    def getValidatedDict(self):
        return {p.tid: p.id for p in self.packs.itervalues()
                            if p.id is not None}

    def notifyCompleted(self, pack_id):
        for tid in [p.tid for p in self.packs.values() if p.id <= pack_id]:
            self.packs.pop(tid).completed()

    def clientLost(self, conn):
        for p in self.packs.itervalues():
            p.forget(waiter)
        self.connectionLost(conn)

    def connectionLost(self, conn):
        self.old = [old for old in self.old if old.connectionLost(conn)]
