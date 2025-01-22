# -*- coding: utf-8 -*-
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

from neo.lib import logging
from neo.lib.exception import PrimaryElected
from neo.lib.handler import MTEventHandler
from neo.lib.pt import MTPartitionTable as PartitionTable
from neo.lib.protocol import NodeStates
from . import AnswerBaseHandler
from ..exception import NEOStorageError


class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def answerLastTransaction(*args):
        pass

class PrimaryNotificationsHandler(MTEventHandler):
    """ Handler that process the notifications from the primary master """

    def notPrimaryMaster(self, *args):
        try:
            super(PrimaryNotificationsHandler, self).notPrimaryMaster(*args)
        except PrimaryElected as e:
            self.app.primary_master_node, = e.args

    def answerLastTransaction(self, conn, ltid):
        app = self.app
        app_last_tid = app.__dict__.get('last_tid', '')
        if app_last_tid != ltid:
            # Either we're connecting or we already know the last tid
            # via invalidations.
            assert app.master_conn is None, app.master_conn
            with app._cache_lock:
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
                app._loading.clear()
            db = app.getDB()
            db is None or db.invalidateCache()
            app.last_tid = ltid
        app.ignore_invalidations = False

    def answerTransactionFinished(self, conn, _, tid, callback, cache_dict):
        app = self.app
        cache = app._cache
        invalidate = cache.invalidate
        loading_get = app._loading.get
        with app._cache_lock:
            for oid, data in cache_dict.iteritems():
                # Update ex-latest value in cache
                invalidate(oid, tid)
                loading = loading_get(oid)
                if loading:
                    loading[1].append(tid)
                if data is not None:
                    # Store in cache with no next_tid
                    cache.store(oid, data, tid, None)
            if callback is not None:
                callback(tid)
            app.last_tid = tid # see comment in invalidateObjects

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
        with app._cache_lock:
            invalidate = app._cache.invalidate
            loading_get = app._loading.get
            for oid in oid_list:
                invalidate(oid, tid)
                loading = loading_get(oid)
                if loading:
                    loading[1].append(tid)
            db = app.getDB()
            if db is not None:
                db.invalidate(tid, oid_list)
            # ZODB<5: Update before releasing the lock so that app.load
            #         asks the last serial (with respect to already processed
            #         invalidations by Connection._setstate).
            # ZODB≥5: Update after db.invalidate because the MVCC
            #         adapter starts at the greatest TID between
            #         IStorage.lastTransaction and processed invalidations.
            app.last_tid = tid

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        pt = self.app.pt = object.__new__(PartitionTable)
        pt.load(ptid, num_replicas, row_list, self.app.nm)

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        self.app.pt.update(ptid, num_replicas, cell_list, self.app.nm)

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

    def waitedForPack(self, conn):
        pass
