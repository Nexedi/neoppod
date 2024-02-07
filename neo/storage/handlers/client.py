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
from neo.lib.exception import NonReadableCell, ProtocolError, UndoPackError
from neo.lib.handler import DelayEvent
from neo.lib.util import dump, makeChecksum, add64
from neo.lib.protocol import Packets, Errors, \
    ZERO_HASH, ZERO_TID, INVALID_PARTITION
from ..transactions import ConflictError, NotRegisteredError
from . import BaseHandler
import time

# Log stores taking (incl. lock delays) more than this many seconds.
# Set to None to disable.
SLOW_STORE = 2

class ClientOperationHandler(BaseHandler):

    def connectionClosed(self, conn):
        logging.debug('connection closed for %r', conn)
        app = self.app
        if app.operational:
            # Even if in most cases, abortFor is called from both this method
            # and BaseMasterHandler.notifyNodeInformation (especially since
            # storage nodes disconnects unknown clients on their own), these 2
            # handlers also cover distinct scenarios, so neither of them is
            # redundant:
            # - A client node may have network issues with this particular
            #   storage node and remain RUNNING: we may still be involved in
            #   the second phase so we only abort non-voted transactions here.
            #   By not taking part to any further deadlock avoidance,
            #   not releasing write-locks now would lead to a deadlock.
            # - A client node may be disconnected from the master, whereas
            #   there are still voted (and not locked) transactions to abort.
            with app.dm.lock:
                app.tm.abortFor(conn.getUUID())

    def askTransactionInformation(self, conn, tid):
        t = self.app.dm.getTransaction(tid)
        if t is None:
            p = Errors.TidNotFound('%s does not exist' % dump(tid))
        else:
            p = Packets.AnswerTransactionInformation(tid, t[1], t[2], t[3],
                    t[4], t[0])
        conn.answer(p)

    def getEventQueue(self):
        # for read rpc
        return self.app.tm.read_queue

    def askObject(self, conn, oid, at, before):
        app = self.app
        if app.tm.loadLocked(oid):
            raise DelayEvent
        o = app.dm.getObject(oid, at, before)
        try:
            serial, next_serial, compression, checksum, data, data_serial = o
        except TypeError:
            p = (Errors.OidDoesNotExist if o is None else
                 Errors.OidNotFound)(dump(oid))
        else:
            if checksum is None:
                checksum = ZERO_HASH
                data = ''
            p = Packets.AnswerObject(oid, serial, next_serial,
                compression, checksum, data, data_serial)
        conn.answer(p)

    def askStoreTransaction(self, conn, ttid, *txn_info):
        self.app.tm.register(conn, ttid)
        self.app.tm.vote(ttid, txn_info)
        conn.answer(Packets.AnswerStoreTransaction())

    def askVoteTransaction(self, conn, ttid):
        self.app.tm.vote(ttid)
        conn.answer(Packets.AnswerVoteTransaction())

    def _askStoreObject(self, conn, oid, serial, compression, checksum, data,
            data_serial, ttid, request_time):
        try:
            locked = self.app.tm.storeObject(ttid, serial, oid, compression,
                    checksum, data, data_serial)
        except ConflictError, err:
            # resolvable or not
            locked = err.tid
        except NonReadableCell:
            logging.info('Ignore store of %s:%s by %s: unassigned partition',
                dump(oid), dump(serial), dump(ttid))
            locked = ZERO_TID
        except NotRegisteredError:
            # transaction was aborted, cancel this event
            logging.info('Forget store of %s:%s by %s delayed by %s',
                    dump(oid), dump(serial), dump(ttid),
                    dump(self.app.tm.getLockingTID(oid)))
            locked = ZERO_TID
        except UndoPackError:
            conn.answer(Errors.UndoPackError(
                'Could not undo for oid %s' % dump(oid)))
            return
        else:
            if request_time and SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('StoreObject delay: %.02fs', duration)
        conn.answer(Packets.AnswerStoreObject(locked))

    def askStoreObject(self, conn, oid, serial,
            compression, checksum, data, data_serial, ttid):
        if 1 < compression:
            raise ProtocolError('invalid compression value')
        # register the transaction
        self.app.tm.register(conn, ttid)
        if data or checksum != ZERO_HASH:
            if makeChecksum(data) != checksum:
                raise ProtocolError('invalid checksum')
        else:
            checksum = data = None
        try:
            self._askStoreObject(conn, oid, serial, compression,
                checksum, data, data_serial, ttid, None)
        except DelayEvent, e:
            # locked by a previous transaction, retry later
            self.app.tm.queueEvent(self._askStoreObject, conn, (oid, serial,
                compression, checksum, data, data_serial, ttid, time.time()),
            *e.args)

    def askRelockObject(self, conn, ttid, oid):
        try:
            self.app.tm.relockObject(ttid, oid, True)
        except DelayEvent, e:
            # locked by a previous transaction, retry later
            self.app.tm.queueEvent(self._askRelockObject,
                conn, (ttid, oid, time.time()), *e.args)
        else:
            conn.answer(Packets.AnswerRelockObject(None))

    def _askRelockObject(self, conn, ttid, oid, request_time):
        conflict = self.app.tm.relockObject(ttid, oid, False)
        if request_time and SLOW_STORE is not None:
            duration = time.time() - request_time
            if duration > SLOW_STORE:
                logging.info('RelockObject delay: %.02fs', duration)
        conn.answer(Packets.AnswerRelockObject(conflict))

    def askTIDsFrom(self, conn, min_tid, max_tid, length, partition):
        conn.answer(Packets.AnswerTIDsFrom(self.app.dm.getReplicationTIDList(
            min_tid, max_tid, length, partition)))

    def _askTIDs(self, first, last, partition):
        # This method is complicated, because I must return TIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise ProtocolError('invalid offsets')

        app = self.app
        if partition == INVALID_PARTITION:
            partition_list = app.pt.getReadableOffsetList(app.uuid)
        else:
            partition_list = [partition]

        return app.dm.getTIDList(first, last - first, partition_list)

    def askTIDs(self, conn, *args):
        conn.answer(Packets.AnswerTIDs(self._askTIDs(*args)))

    def askFinalTID(self, conn, ttid):
        conn.answer(Packets.AnswerFinalTID(self.app.tm.getFinalTID(ttid)))

    def askObjectUndoSerial(self, conn, ttid, ltid, undone_tid, oid_list):
        app = self.app
        findUndoTID = app.dm.findUndoTID
        getObjectFromTransaction = app.tm.getObjectFromTransaction
        object_tid_dict = {}
        for oid in oid_list:
            transaction_object = getObjectFromTransaction(ttid, oid)
            r = findUndoTID(oid, ltid, undone_tid,
                transaction_object and (transaction_object[2] or ttid))
            if r:
                if not r[0]:
                    p = Errors.OidNotFound(dump(oid))
                    break
                object_tid_dict[oid] = r
        else:
            p = Packets.AnswerObjectUndoSerial(object_tid_dict)
        conn.answer(p)

    def askObjectHistory(self, conn, oid, first, last):
        if first >= last:
            raise ProtocolError('invalid offsets')
        app = self.app
        if app.tm.loadLocked(oid):
            raise DelayEvent
        history_list = app.dm.getObjectHistoryWithLength(
            oid, first, last - first)
        if history_list is None:
            p = Errors.OidNotFound(dump(oid))
        else:
            p = Packets.AnswerObjectHistory(oid, history_list)
        conn.answer(p)

    def askCheckCurrentSerial(self, conn, ttid, oid, serial):
        self.app.tm.register(conn, ttid)
        try:
            self._askCheckCurrentSerial(conn, ttid, oid, serial, None)
        except DelayEvent, e:
            # locked by a previous transaction, retry later
            self.app.tm.queueEvent(self._askCheckCurrentSerial,
                conn, (ttid, oid, serial, time.time()), *e.args)

    def _askCheckCurrentSerial(self, conn, ttid, oid, serial, request_time):
        try:
            locked = self.app.tm.checkCurrentSerial(ttid, oid, serial)
        except ConflictError, err:
            # resolvable or not
            locked = err.tid
        except NonReadableCell:
            logging.info('Ignore check of %s:%s by %s: unassigned partition',
                dump(oid), dump(serial), dump(ttid))
            locked = ZERO_TID
        except NotRegisteredError:
            # transaction was aborted, cancel this event
            logging.info('Forget serial check of %s:%s by %s delayed by %s',
                dump(oid), dump(serial), dump(ttid),
                dump(self.app.tm.getLockingTID(oid)))
            locked = ZERO_TID
        else:
            if request_time and SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('CheckCurrentSerial delay: %.02fs', duration)
        conn.answer(Packets.AnswerCheckCurrentSerial(locked))

    def askOIDsFrom(self, conn, partition, length, min_oid, tid):
        app = self.app
        if app.tm.isLockedTid(tid):
            raise DelayEvent
        conn.answer(Packets.AnswerOIDsFrom(
            *app.dm.oidsFrom(partition, length, min_oid, tid)))


# like ClientOperationHandler but read-only & only for tid <= backup_tid
class ClientReadOnlyOperationHandler(ClientOperationHandler):

    def _readOnly(self, conn, *args, **kw):
        conn.answer(Errors.ReadOnlyAccess(
            'read-only access because cluster is in backuping mode'))

    askStoreTransaction     = _readOnly
    askVoteTransaction      = _readOnly
    askStoreObject          = _readOnly
    askFinalTID             = _readOnly
    askRelockObject         = _readOnly
    # takes write lock & is only used when going to commit
    askCheckCurrentSerial   = _readOnly

    # below operations: like in ClientOperationHandler but cut tid <= backup_tid

    def askTransactionInformation(self, conn, tid):
        backup_tid = self.app.dm.getBackupTID()
        if tid > backup_tid:
            conn.answer(Errors.TidNotFound(
                'tids > %s are not fully fetched yet' % dump(backup_tid)))
            return
        super(ClientReadOnlyOperationHandler, self).askTransactionInformation(
            conn, tid)

    def askObject(self, conn, oid, serial, tid):
        backup_tid = self.app.dm.getBackupTID()
        if serial:
            if serial > backup_tid:
                # obj lookup will find nothing, but return properly either
                # OidDoesNotExist or OidNotFound
                serial = ZERO_TID
        elif tid:
            tid = min(tid, add64(backup_tid, 1))

        # limit "latest obj" query to tid <= backup_tid
        else:
            tid = add64(backup_tid, 1)

        super(ClientReadOnlyOperationHandler, self).askObject(
            conn, oid, serial, tid)

    def askTIDsFrom(self, conn, min_tid, max_tid, length, partition):
        backup_tid = self.app.dm.getBackupTID()
        max_tid = min(max_tid, backup_tid)
        # NOTE we don't need to adjust min_tid: if min_tid > max_tid
        #      db.getReplicationTIDList will return empty [], which is correct
        super(ClientReadOnlyOperationHandler, self).askTIDsFrom(
                conn, min_tid, max_tid, length, partition)

    def askTIDs(self, conn, first, last, partition):
        backup_tid = self.app.dm.getBackupTID()
        tid_list = self._askTIDs(first, last, partition)
        tid_list = filter(lambda tid: tid <= backup_tid, tid_list)
        conn.answer(Packets.AnswerTIDs(tid_list))

    # FIXME askObjectUndoSerial to limit tid <= backup_tid
    # (askObjectUndoSerial is used in undo() but itself is read-only query)

    # FIXME askObjectHistory to limit tid <= backup_tid
    # TODO dm.getObjectHistoryWithLength has to be first fixed for this
    #def askObjectHistory(self, conn, oid, first, last):
