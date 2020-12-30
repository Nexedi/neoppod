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
from neo.lib.util import add64


class Pack(object):

    def __init__(self, tid, approved, partial, oids, time):
        self.tid = tid
        self.approved = approved
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

    def __init__(self, app, pack_id, only_first_approved, caller):
        self.app = proxy(app)
        self.caller = caller
        self.pack_id = pack_id
        self.only_first_approved = only_first_approved
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
        p = Packets.AskPackOrders(self.pack_id)
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
                self.caller(pm.dump(self.pack_id, self.only_first_approved))
                pm.notifyCompleted(app.getCompletedPackId())


class PackManager(object):

    autosign = True

    def __init__(self):
        self.max_completed = None
        self.packs = {}
        self.old = []

    reset = __init__

    def add(self, tid, *args):
        p = self.packs.get(tid)
        if p is None:
            self.packs[tid] = Pack(tid, *args)
            if None is not self.max_completed > tid:
                self.max_completed = add64(tid, -1)
        elif p.approved is None:
            p.approved = args[0]

    @apply
    def dump():
        by_tid = attrgetter('tid')
        def dump(self, pack_id, only_first_approved):
            if only_first_approved:
                try:
                    p = min((p for p in self.packs.itervalues()
                           if p.approved and p.tid >= pack_id),
                        key=by_tid),
                except ValueError:
                    p = ()
            else:
                p = sorted(
                    (p for p in self.packs.itervalues() if p.tid >= pack_id),
                    key=by_tid)
            return [(p.tid, p.approved, p.partial, p.oids, p.time) for p in p]
        return dump

    def new(self, tid, oids, time):
        autosign = self.autosign
        self.packs[tid] = Pack(tid, autosign or None, bool(oids), oids, time)
        return autosign

    def getApprovedRejected(self):
        r = [], []
        for p in self.packs.itervalues():
            approved = p.approved
            if approved is not None:
                r[approved].append(p.tid)
        return r

    def notifyCompleted(self, pack_id):
        for tid in list(self.packs):
            if tid <= pack_id:
                self.packs.pop(tid).completed()
                if self.max_completed is None or self.max_completed < tid:
                    self.max_completed = tid

    def clientLost(self, conn):
        for p in self.packs.itervalues():
            p.forget(waiter)
        self.connectionLost(conn)

    def connectionLost(self, conn):
        self.old = [old for old in self.old if old.connectionLost(conn)]
