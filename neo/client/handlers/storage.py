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

from zlib import decompress
from ZODB.TimeStamp import TimeStamp

from neo.lib import logging
from neo.lib.protocol import Packets, uuid_str
from neo.lib.util import dump, makeChecksum
from neo.lib.exception import NodeNotReady
from neo.lib.handler import MTEventHandler
from . import AnswerBaseHandler
from ..transactions import Transaction
from ..exception import NEOStorageError, NEOStorageNotFoundError
from ..exception import NEOStorageReadRetry, NEOStorageDoesNotExistError

class StorageEventHandler(MTEventHandler):

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByAddress(conn.getAddress())
        assert node is not None
        self.app.cp.removeConnection(node)
        super(StorageEventHandler, self).connectionLost(conn, new_state)

    def connectionFailed(self, conn):
        # Connection to a storage node failed
        node = self.app.nm.getByAddress(conn.getAddress())
        assert node is not None
        self.app.cp.removeConnection(node)
        super(StorageEventHandler, self).connectionFailed(conn)

    def _acceptIdentification(*args):
        pass

class StorageBootstrapHandler(AnswerBaseHandler):
    """ Handler used when connecting to a storage node """

    def notReady(self, conn, message):
        conn.close()
        raise NodeNotReady(message)

class StorageAnswersHandler(AnswerBaseHandler):
    """ Handle all messages related to ZODB operations """

    def answerObject(self, conn, oid, *args):
        self.app.setHandlerData(args)

    def answerStoreObject(self, conn, conflict, oid):
        txn_context = self.app.getHandlerData()
        if conflict:
            # Conflicts can not be resolved now because 'conn' is locked.
            # We must postpone the resolution (by queuing the conflict in
            # 'conflict_dict') to avoid any deadlock with another thread that
            # also resolves a conflict successfully to the same storage nodes.
            # Warning: if a storage (S1) is much faster than another (S2), then
            # we may process entirely a conflict with S1 (i.e. we received the
            # answer to the store of the resolved object on S1) before we
            # receive the conflict answer from the first store on S2.
            logging.info('%s reports a conflict on %s:%s with %s',
                         uuid_str(conn.getUUID()), dump(oid),
                         dump(txn_context.ttid), dump(conflict))
            # If this conflict is not already resolved, mark it for
            # resolution.
            if  txn_context.resolved_dict.get(oid, '') < conflict:
                txn_context.conflict_dict[oid] = conflict
        else:
            txn_context.written(self.app, conn.getUUID(), oid)

    answerCheckCurrentSerial = answerStoreObject

    def answerRebaseTransaction(self, conn, oid_list):
        txn_context = self.app.getHandlerData()
        ttid = txn_context.ttid
        queue = txn_context.queue
        try:
            for oid in oid_list:
                # We could have an extra parameter to tell the storage if we
                # still have the data, and in this case revert what was done
                # in Transaction.written. This would save bandwidth in case of
                # conflict.
                conn.ask(Packets.AskRebaseObject(ttid, oid),
                         queue=queue, oid=oid)
        except ConnectionClosed:
            txn_context.involved_nodes[conn.getUUID()] = 2

    def answerRebaseObject(self, conn, conflict, oid):
        if conflict:
            txn_context = self.app.getHandlerData()
            serial, conflict, data = conflict
            assert serial and serial < conflict, (serial, conflict)
            resolved = conflict <= txn_context.resolved_dict.get(oid, '')
            try:
                cached = txn_context.cache_dict.pop(oid)
            except KeyError:
                if resolved:
                    # We should still be waiting for an answer from this node.
                    assert conn.uuid in txn_context.data_dict[oid][2]
                    return
                assert oid in txn_context.data_dict
                if serial <= txn_context.conflict_dict.get(oid, ''):
                    # Another node already reported this conflict or a newer,
                    # by answering to this rebase or to the previous store.
                    return
                # A node has not answered yet to a previous store. Do not wait
                # it to report the conflict because it may fail before.
            else:
                # The data for this oid are now back on client side.
                # Revert what was done in Transaction.written
                assert not resolved
                if data is None: # undo or CHECKED_SERIAL
                    data = cached
                else:
                    compression, checksum, data = data
                    if checksum != makeChecksum(data):
                        raise NEOStorageError(
                            'wrong checksum while getting back data for'
                            ' object %s during rebase of transaction %s'
                            % (dump(oid), dump(txn_context.ttid)))
                    if compression:
                        data = decompress(data)
                    size = len(data)
                    txn_context.data_size += size
                    if cached:
                        assert cached == data
                        txn_context.cache_size -= size
                txn_context.data_dict[oid] = data, serial, None
            txn_context.conflict_dict[oid] = conflict

    def answerStoreTransaction(self, conn):
        pass

    answerVoteTransaction = answerStoreTransaction

    def connectionClosed(self, conn):
        txn_context = self.app.getHandlerData()
        if type(txn_context) is Transaction:
            txn_context.nodeLost(self.app, conn.getUUID())
        super(StorageAnswersHandler, self).connectionClosed(conn)

    def answerTIDsFrom(self, conn, tid_list):
        logging.debug('Get %u TIDs from %r', len(tid_list), conn)
        self.app.setHandlerData(tid_list)

    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        self.app.setHandlerData(({
            'time': TimeStamp(tid).timeTime(),
            'user_name': user,
            'description': desc,
            'id': tid,
            'oids': oid_list,
            'packed': packed,
        }, ext))

    def answerObjectHistory(self, conn, _, history_list):
        # history_list is a list of tuple (serial, size)
        self.app.setHandlerData(history_list)

    def oidNotFound(self, conn, message):
        # This can happen either when :
        # - loading an object
        # - asking for history
        raise NEOStorageNotFoundError(message)

    def oidDoesNotExist(self, conn, message):
        raise NEOStorageDoesNotExistError(message)

    def tidNotFound(self, conn, message):
        # This can happen when requiring txn informations
        raise NEOStorageNotFoundError(message)

    def nonReadableCell(self, conn, message):
        logging.info('non readable cell')
        raise NEOStorageReadRetry(True)

    def answerTIDs(self, conn, tid_list, tid_set):
        tid_set.update(tid_list)

    def answerObjectUndoSerial(self, conn, object_tid_dict, partition,
                               partition_oid_dict, undo_object_tid_dict):
        del partition_oid_dict[partition]
        undo_object_tid_dict.update(object_tid_dict)

    def answerFinalTID(self, conn, tid):
        self.app.setHandlerData(tid)
