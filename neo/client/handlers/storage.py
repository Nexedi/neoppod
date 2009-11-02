#
# Copyright (C) 2006-2009  Nexedi SA
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

from neo import logging
from ZODB.TimeStamp import TimeStamp

from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.protocol import NodeTypes

class StorageEventHandler(BaseHandler):

    def _dealWithStorageFailure(self, conn):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        assert node is not None
        # Remove from pool connection
        app.cp.removeConnection(node)
        app.dispatcher.unregister(conn) 

    def connectionLost(self, conn, new_state):
        self._dealWithStorageFailure(conn)

    def connectionFailed(self, conn):
        # XXX: a connection failure is not like a connection lost, we should not
        # have to clear the dispatcher because the connection was never
        # established and so, no packet should have been send and thus, nothing
        # must be expected. This should be well done if the first packet sent is
        # done after the connectionCompleted event or a packet received.
        # Connection to a storage node failed
        self._dealWithStorageFailure(conn)
        super(StorageEventHandler, self).connectionFailed(conn)


class StorageBootstrapHandler(AnswerBaseHandler):
    """ Handler used when connecting to a storage node """

    def notReady(self, conn, packet, message):
        app = self.app
        app.setNodeNotReady()
        
    def acceptIdentification(self, conn, packet, node_type,
           uuid, address, num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        # this must be a storage node
        if node_type != NodeTypes.STORAGE:
            conn.close()
            return
        if conn.getAddress() != address:
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                  conn.getAddress()[0], conn.getAddress()[1], *address)
            app.nm.remove(node)
            conn.close()
            return

        conn.setUUID(uuid)
        node.setUUID(uuid)

class StorageAnswersHandler(AnswerBaseHandler):
    """ Handle all messages related to ZODB operations """
        
    def answerObject(self, conn, packet, oid, start_serial, end_serial, 
            compression, checksum, data):
        app = self.app
        app.local_var.asked_object = (oid, start_serial, end_serial, 
                compression, checksum, data)

    def answerStoreObject(self, conn, packet, conflicting, oid, serial):
        app = self.app
        if conflicting:
            app.local_var.object_stored = -1, serial
        else:
            app.local_var.object_stored = oid, serial

    def answerStoreTransaction(self, conn, packet, tid):
        app = self.app
        app.setTransactionVoted()

    def answerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        app = self.app
        # transaction information are returned as a dict
        info = {}
        info['time'] = TimeStamp(tid).timeTime()
        info['user_name'] = user
        info['description'] = desc
        info['id'] = tid
        info['oids'] = oid_list
        app.local_var.txn_info = info

    def answerObjectHistory(self, conn, packet, oid, history_list):
        app = self.app
        # history_list is a list of tuple (serial, size)
        app.local_var.history = oid, history_list

    def oidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen either when :
        # - loading an object
        # - asking for history
        app.local_var.asked_object = -1
        app.local_var.history = -1

    def tidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen when requiring txn informations
        app.local_var.txn_info = -1

    def answerTIDs(self, conn, packet, tid_list):
        app = self.app
        app.local_var.node_tids[conn.getUUID()] = tid_list

