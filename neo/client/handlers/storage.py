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

from neo import logging
from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.protocol import NodeTypes, ProtocolError, LockState
from neo.util import dump
from neo.client.exception import NEOStorageError

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
        self.app.setNodeNotReady()

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
        self.app.local_var.asked_object = (oid, start_serial, end_serial,
                compression, checksum, data)

    def answerStoreObject(self, conn, conflicting, oid, serial):
        local_var = self.app.local_var
        object_stored_counter_dict = local_var.object_stored_counter_dict[oid]
        if conflicting:
            logging.info('%r report a conflict for %r with %r', conn,
                        dump(oid), dump(serial))
            conflict_serial_dict = local_var.conflict_serial_dict
            if serial in object_stored_counter_dict:
                raise NEOStorageError, 'A storage accepted object for ' \
                    'serial %s but another reports a conflict for it.' % (
                        dump(serial), )
            # If this conflict is not already resolved, mark it for
            # resolution.
            if serial not in local_var.resolved_conflict_serial_dict.get(oid, ()):
                conflict_serial_dict.setdefault(oid, set()).add(serial)
        else:
            object_stored_counter_dict[serial] = \
                object_stored_counter_dict.get(serial, 0) + 1

    def answerStoreTransaction(self, conn, tid):
        if tid != self.app.getTID():
            raise ProtocolError('Wrong TID, transaction not started')
        self.app.setTransactionVoted()

    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        # transaction information are returned as a dict
        info = {}
        info['time'] = TimeStamp(tid).timeTime()
        info['user_name'] = user
        info['description'] = desc
        info['id'] = tid
        info['oids'] = oid_list
        info['packed'] = packed
        self.app.local_var.txn_ext = ext
        self.app.local_var.txn_info = info

    def answerObjectHistory(self, conn, oid, history_list):
        # history_list is a list of tuple (serial, size)
        self.app.local_var.history = oid, history_list

    def oidNotFound(self, conn, message):
        # This can happen either when :
        # - loading an object
        # - asking for history
        self.app.local_var.asked_object = -1
        self.app.local_var.history = -1

    def tidNotFound(self, conn, message):
        # This can happen when requiring txn informations
        self.app.local_var.txn_info = -1

    def answerTIDs(self, conn, tid_list):
        self.app.local_var.node_tids[conn.getUUID()] = tid_list

    def answerUndoTransaction(self, conn, oid_list, error_oid_list,
            conflict_oid_list):
        local_var = self.app.local_var
        local_var.undo_conflict_oid_list.extend(conflict_oid_list)
        local_var.undo_error_oid_list.extend(error_oid_list)
        data_dict = local_var.data_dict
        for oid in oid_list:
            data_dict[oid] = ''

    def answerHasLock(self, conn, oid, status):
        if status == LockState.GRANTED_TO_OTHER:
            # Object is locked by another transaction, and we have waited until
            # timeout. To avoid a deadlock, abort current transaction (we might
            # be locking objects the other transaction is waiting for).
            raise ConflictError, 'Lock wait timeout for oid %s on %r' % (
                dump(oid), conn)
        elif status == LockState.GRANTED:
            logging.info('Store of oid %s was successful, but after timeout.',
                dump(oid))
            # XXX: Not sure what to do in this case yet, for now do nothing.
        else:
            # Nobody has the lock, although we asked storage to lock. This
            # means there is a software bug somewhere.
            # XXX: Not sure what to do in this case yet
            raise NotImplementedError

