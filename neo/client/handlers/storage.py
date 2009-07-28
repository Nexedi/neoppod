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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from neo import logging
from ZODB.TimeStamp import TimeStamp

from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo import protocol

class StorageEventHandler(BaseHandler):

    def _dealWithStorageFailure(self, conn, node):
        app = self.app

        # Remove from pool connection
        app.cp.removeConnection(node)

        # Put fake packets to task queues.
        queue_set = set()
        for key in self.dispatcher.message_table.keys():
            if id(conn) == key[0]:
                queue = self.dispatcher.message_table.pop(key)
                queue_set.add(queue)
        # Storage failure is notified to the primary master when the fake 
        # packet if popped by a non-polling thread.
        for queue in queue_set:
            queue.put((conn, None))


    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        logging.info("connection to storage node %s closed", node.getServer())
        self._dealWithStorageFailure(conn, node)
        super(StorageEventHandler, self).connectionClosed(conn)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node)
        super(StorageEventHandler, self).timeoutExpired(conn)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node)
        super(StorageEventHandler, self).peerBroken(conn)

    def connectionFailed(self, conn):
        # Connection to a storage node failed
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node)
        super(StorageEventHandler, self).connectionFailed(conn)

class StorageBootstrapHandler(AnswerBaseHandler):
    """ Handler used when connecting to a storage node """

    def handleNotReady(self, conn, packet, message):
        app = self.app
        app.setNodeNotReady()
        
    def handleAcceptNodeIdentification(self, conn, packet, node_type,
           uuid, address, num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        # It can be eiter a master node or a storage node
        if node_type != protocol.STORAGE_NODE_TYPE:
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
        
    def handleAnswerObject(self, conn, packet, oid, start_serial, end_serial, 
            compression, checksum, data):
        app = self.app
        app.local_var.asked_object = (oid, start_serial, end_serial, compression,
                                      checksum, data)

    def handleAnswerStoreObject(self, conn, packet, conflicting, oid, serial):
        app = self.app
        if conflicting:
            app.local_var.object_stored = -1, serial
        else:
            app.local_var.object_stored = oid, serial

    def handleAnswerStoreTransaction(self, conn, packet, tid):
        app = self.app
        app.setTransactionVoted()

    def handleAnswerTransactionInformation(self, conn, packet, tid,
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

    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        app = self.app
        # history_list is a list of tuple (serial, size)
        app.local_var.history = oid, history_list

    def handleOidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen either when :
        # - loading an object
        # - asking for history
        app.local_var.asked_object = -1
        app.local_var.history = -1

    def handleTidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen when requiring txn informations
        app.local_var.txn_info = -1

    def handleAnswerTIDs(self, conn, packet, tid_list):
        app = self.app
        app.local_var.node_tids[conn.getUUID()] = tid_list

