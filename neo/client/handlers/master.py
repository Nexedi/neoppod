#
# Copyright (C) 2006-2017  Nexedi SA
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

from neo.lib import logging
from neo.lib.exception import PrimaryElected
from neo.lib.handler import MTEventHandler
from neo.lib.pt import MTPartitionTable as PartitionTable
from neo.lib.protocol import NodeStates
from . import AnswerBaseHandler
from ..exception import NEOStorageError


class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def answerPartitionTable(self, conn, ptid, row_list):
        assert row_list
        self.app.pt.load(ptid, row_list, self.app.nm)

    def answerLastTransaction(*args):
        pass

class PrimaryNotificationsHandler(MTEventHandler):
    """ Handler that process the notifications from the primary master """

    def notPrimaryMaster(self, *args):
        try:
            super(PrimaryNotificationsHandler, self).notPrimaryMaster(*args)
        except PrimaryElected, e:
            self.app.primary_master_node, = e.args

    def _acceptIdentification(self, node, num_partitions, num_replicas):
        self.app.pt = PartitionTable(num_partitions, num_replicas)

    def answerLastTransaction(self, conn, ltid):
        app = self.app
        app_last_tid = app.__dict__.get('last_tid', '')
        if app_last_tid != ltid:
            # Either we're connecting or we already know the last tid
            # via invalidations.
            assert app.master_conn is None, app.master_conn
            app._cache_lock_acquire()
            try:
                if app_last_tid < ltid:
                    app._cache.clear_current()
                    # In the past, we tried not to invalidate the
                    # Connection caches entirely, using the list of
                    # oids that are invalidated by clear_current.
                    # This was wrong because these caches may have
                    # entries that are not in the NEO cache anymore.
                else:
                    # The DB was truncated. It happens so
                    # rarely that we don't need to optimize.
                    app._cache.clear()
                # Make sure a parallel load won't refill the cache
                # with garbage.
                app._loading_oid = app._loading_invalidated = None
            finally:
                app._cache_lock_release()
            db = app.getDB()
            db is None or db.invalidateCache()
            app.last_tid = ltid
        app.ignore_invalidations = False

    def answerTransactionFinished(self, conn, _, tid, callback, cache_dict):
        app = self.app
        app.last_tid = tid
        # Update cache
        cache = app._cache
        app._cache_lock_acquire()
        try:
            for oid, data in cache_dict.iteritems():
                # Update ex-latest value in cache
                cache.invalidate(oid, tid)
                if data is not None:
                    # Store in cache with no next_tid
                    cache.store(oid, data, tid, None)
            if callback is not None:
                callback(tid)
        finally:
            app._cache_lock_release()

    def connectionClosed(self, conn):
        app = self.app
        if app.master_conn is not None:
            msg = "connection to primary master node closed"
            logging.critical(msg)
            app.master_conn = None
            for txn_context in app.txn_contexts():
                txn_context.error = msg
        try:
            app.__dict__.pop('pt').clear()
        except KeyError:
            pass
        app.primary_master_node = None
        super(PrimaryNotificationsHandler, self).connectionClosed(conn)

    def stopOperation(self, conn):
        logging.critical("master node ask to stop operation")

    def notifyClusterInformation(self, conn, state):
        # TODO: on shutdown, abort any transaction that is not voted
        logging.info("cluster switching to %s state", state)

    def invalidateObjects(self, conn, tid, oid_list):
        app = self.app
        if app.ignore_invalidations:
            return
        app.last_tid = tid
        app._cache_lock_acquire()
        try:
            invalidate = app._cache.invalidate
            loading = app._loading_oid
            for oid in oid_list:
                invalidate(oid, tid)
                if oid == loading:
                    app._loading_invalidated.append(tid)
            db = app.getDB()
            if db is not None:
                db.invalidate(tid, oid_list)
        finally:
            app._cache_lock_release()

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        if self.app.pt.filled():
            self.app.pt.update(ptid, cell_list, self.app.nm)

    def notifyNodeInformation(self, conn, timestamp, node_list):
        super(PrimaryNotificationsHandler, self).notifyNodeInformation(
            conn, timestamp, node_list)
        # XXX: 'update' automatically closes UNKNOWN nodes. Do we really want
        #      to do the same thing for nodes in other non-running states ?
        getByUUID = self.app.nm.getByUUID
        for node in node_list:
            if node[3] != NodeStates.RUNNING:
                node = getByUUID(node[2])
                if node and node.isConnected():
                    node.getConnection().close()

    def notifyDeadlock(self, conn, ttid, locking_tid):
        for txn_context in self.app.txn_contexts():
            if txn_context.ttid == ttid:
                txn_context.conflict_dict[None] = locking_tid
                txn_context.wakeup(conn)
                break

class PrimaryAnswersHandler(AnswerBaseHandler):
    """ Handle that process expected packets from the primary master """

    def answerBeginTransaction(self, conn, ttid):
        self.app.setHandlerData(ttid)

    def answerNewOIDs(self, conn, oid_list):
        self.app.new_oids = iter(oid_list)

    def incompleteTransaction(self, conn, message):
        raise NEOStorageError("storage nodes for which vote failed can not be"
            " disconnected without making the cluster non-operational")

    def answerTransactionFinished(self, conn, _, tid):
        self.app.setHandlerData(tid)

    def answerPack(self, conn, status):
        if not status:
            raise NEOStorageError('Already packing')

    def answerLastTransaction(self, conn, ltid):
        pass

    def answerFinalTID(self, conn, tid):
        self.app.setHandlerData(tid)
