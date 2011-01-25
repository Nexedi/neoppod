#
# Copyright (C) 2006-2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from ZODB.TimeStamp import TimeStamp
from ZODB.POSException import ConflictError

import neo.lib
from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.lib.protocol import NodeTypes, ProtocolError, LockState
from neo.lib.util import dump
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError
from neo.lib.exception import NodeNotReady

class StorageEventHandler(BaseHandler):

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByAddress(conn.getAddress())
        assert node is not None
        self.app.cp.removeConnection(node)
        self.app.dispatcher.unregister(conn)

    def connectionFailed(self, conn):
        # Connection to a storage node failed
        node = self.app.nm.getByAddress(conn.getAddress())
        assert node is not None
        self.app.cp.removeConnection(node)
        super(StorageEventHandler, self).connectionFailed(conn)


class StorageBootstrapHandler(AnswerBaseHandler):
    """ Handler used when connecting to a storage node """

    def notReady(self, conn, message):
        raise NodeNotReady(message)

    def acceptIdentification(self, conn, node_type,
           uuid, num_partitions, num_replicas, your_uuid):
        # this must be a storage node
        if node_type != NodeTypes.STORAGE:
            conn.close()
            return

        node = self.app.nm.getByAddress(conn.getAddress())
        assert node is not None, conn.getAddress()
        conn.setUUID(uuid)
        node.setUUID(uuid)

class StorageAnswersHandler(AnswerBaseHandler):
    """ Handle all messages related to ZODB operations """

    def answerObject(self, conn, oid, start_serial, end_serial,
            compression, checksum, data, data_serial):
        if data_serial is not None:
            raise NEOStorageError, 'Storage should never send non-None ' \
                'data_serial to clients, got %s' % (dump(data_serial), )
        self.app.setHandlerData((oid, start_serial, end_serial,
                compression, checksum, data))

    def answerStoreObject(self, conn, conflicting, oid, serial):
        txn_context = self.app.getHandlerData()
        object_stored_counter_dict = txn_context[
            'object_stored_counter_dict'][oid]
        if conflicting:
            neo.lib.logging.info('%r report a conflict for %r with %r', conn,
                        dump(oid), dump(serial))
            conflict_serial_dict = txn_context['conflict_serial_dict']
            if serial in object_stored_counter_dict:
                raise NEOStorageError, 'A storage accepted object for ' \
                    'serial %s but another reports a conflict for it.' % (
                        dump(serial), )
            # If this conflict is not already resolved, mark it for
            # resolution.
            if serial not in txn_context[
                    'resolved_conflict_serial_dict'].get(oid, ()):
                conflict_serial_dict.setdefault(oid, set()).add(serial)
        else:
            object_stored_counter_dict[serial] = \
                object_stored_counter_dict.get(serial, 0) + 1

    answerCheckCurrentSerial = answerStoreObject

    def answerStoreTransaction(self, conn, _):
        pass

    def answerTIDsFrom(self, conn, tid_list):
        neo.lib.logging.debug('Get %d TIDs from %r', len(tid_list), conn)
        tids_from = self.app.getHandlerData()
        assert not tids_from.intersection(set(tid_list))
        tids_from.update(tid_list)

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
        self.app.getHandlerData().update(history_list)

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

    def answerTIDs(self, conn, tid_list):
        self.app.getHandlerData().update(tid_list)

    def answerObjectUndoSerial(self, conn, object_tid_dict):
        self.app.getHandlerData().update(object_tid_dict)

    def answerHasLock(self, conn, oid, status):
        if status == LockState.GRANTED_TO_OTHER:
            # Object is locked by another transaction, and we have waited until
            # timeout. To avoid a deadlock, abort current transaction (we might
            # be locking objects the other transaction is waiting for).
            raise ConflictError, 'Lock wait timeout for oid %s on %r' % (
                dump(oid), conn)
        elif status == LockState.GRANTED:
            neo.lib.logging.info('Store of oid %s was successful, but after ' \
                'timeout.', dump(oid))
            # XXX: Not sure what to do in this case yet, for now do nothing.
        else:
            # Nobody has the lock, although we asked storage to lock. This
            # means there is a software bug somewhere.
            # XXX: Not sure what to do in this case yet
            raise NotImplementedError

    def alreadyPendingError(self, conn, message):
        pass

