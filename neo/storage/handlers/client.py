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
from neo.lib.handler import DelayEvent
from neo.lib.util import dump, makeChecksum, add64
from neo.lib.protocol import Packets, Errors, NonReadableCell, ProtocolError, \
    ZERO_HASH, INVALID_PARTITION
from ..transactions import ConflictError, NotRegisteredError
from . import BaseHandler
import time

# Log stores taking (incl. lock delays) more than this many seconds.
# Set to None to disable.
SLOW_STORE = 2

class ClientOperationHandler(BaseHandler):

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

    def askObject(self, conn, oid, serial, tid):
        app = self.app
        if app.tm.loadLocked(oid):
            raise DelayEvent
        o = app.dm.getObject(oid, serial, tid)
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
            self.app.tm.storeObject(ttid, serial, oid, compression,
                    checksum, data, data_serial)
        except ConflictError, err:
            # resolvable or not
            conn.answer(Packets.AnswerStoreObject(err.tid))
            return
        except NonReadableCell:
            logging.info('Ignore store of %s:%s by %s: unassigned partition',
                dump(oid), dump(serial), dump(ttid))
        except NotRegisteredError:
            # transaction was aborted, cancel this event
            logging.info('Forget store of %s:%s by %s delayed by %s',
                    dump(oid), dump(serial), dump(ttid),
                    dump(self.app.tm.getLockingTID(oid)))
        else:
            if request_time and SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('StoreObject delay: %.02fs', duration)
        conn.answer(Packets.AnswerStoreObject(None))

    def askStoreObject(self, conn, oid, serial,
            compression, checksum, data, data_serial, ttid):
        if 1 < compression:
            raise ProtocolError('invalid compression value')
        # register the transaction
        self.app.tm.register(conn, ttid)
        if data or checksum != ZERO_HASH:
            # TODO: return an appropriate error packet
            assert makeChecksum(data) == checksum
            assert data_serial is None
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

    def askRebaseTransaction(self, conn, *args):
        conn.answer(Packets.AnswerRebaseTransaction(
            self.app.tm.rebase(conn, *args)))

    def askRebaseObject(self, conn, ttid, oid):
        try:
            self._askRebaseObject(conn, ttid, oid, None)
        except DelayEvent, e:
            # locked by a previous transaction, retry later
            self.app.tm.queueEvent(self._askRebaseObject,
                conn, (ttid, oid, time.time()), *e.args)

    def _askRebaseObject(self, conn, ttid, oid, request_time):
        conflict = self.app.tm.rebaseObject(ttid, oid)
        if request_time and SLOW_STORE is not None:
            duration = time.time() - request_time
            if duration > SLOW_STORE:
                logging.info('RebaseObject delay: %.02fs', duration)
        conn.answer(Packets.AnswerRebaseObject(conflict))

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
            partition_list = app.pt.getAssignedPartitionList(app.uuid)
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
            current_serial, undo_serial, is_current = findUndoTID(oid, ttid,
                ltid, undone_tid, getObjectFromTransaction(ttid, oid))
            if current_serial is None:
                p = Errors.OidNotFound(dump(oid))
                break
            object_tid_dict[oid] = (current_serial, undo_serial, is_current)
        else:
            p = Packets.AnswerObjectUndoSerial(object_tid_dict)
        conn.answer(p)

    def askObjectHistory(self, conn, oid, first, last):
        if first >= last:
            raise ProtocolError('invalid offsets')
        app = self.app
        if app.tm.loadLocked(oid):
            raise DelayEvent
        history_list = app.dm.getObjectHistory(oid, first, last - first)
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
            self.app.tm.checkCurrentSerial(ttid, oid, serial)
        except ConflictError, err:
            # resolvable or not
            conn.answer(Packets.AnswerCheckCurrentSerial(err.tid))
            return
        except NonReadableCell:
            logging.info('Ignore check of %s:%s by %s: unassigned partition',
                dump(oid), dump(serial), dump(ttid))
        except NotRegisteredError:
            # transaction was aborted, cancel this event
            logging.info('Forget serial check of %s:%s by %s delayed by %s',
                dump(oid), dump(serial), dump(ttid),
                dump(self.app.tm.getLockingTID(oid)))
        else:
            if request_time and SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('CheckCurrentSerial delay: %.02fs', duration)
        conn.answer(Packets.AnswerCheckCurrentSerial(None))


# like ClientOperationHandler but read-only & only for tid <= backup_tid
class ClientReadOnlyOperationHandler(ClientOperationHandler):

    def _readOnly(self, conn, *args, **kw):
        conn.answer(Errors.ReadOnlyAccess(
            'read-only access because cluster is in backuping mode'))

    askStoreTransaction     = _readOnly
    askVoteTransaction      = _readOnly
    askStoreObject          = _readOnly
    askFinalTID             = _readOnly
    askRebaseObject         = _readOnly
    askRebaseTransaction    = _readOnly
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
    # TODO dm.getObjectHistory has to be first fixed for this
    #def askObjectHistory(self, conn, oid, first, last):
