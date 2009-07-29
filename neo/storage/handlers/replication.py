
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

from neo.storage.handlers import BaseStorageHandler
from neo import protocol

class ReplicationHandler(BaseStorageHandler):
    """This class handles events for replications."""

    def connectionCompleted(self, conn):
        # Nothing to do.
        pass

    def handleConnectionLost(self, conn, new_state):
        logging.error('replication is stopped due to a connection lost')
        self.app.replicator.reset()

    def connectionFailed(self, conn):
        logging.error('replication is stopped due to connection failure')
        self.app.replicator.reset()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                       uuid, address, num_partitions, num_replicas, your_uuid):
        # set the UUID on the connection
        conn.setUUID(uuid)

    def handleAnswerTIDs(self, conn, packet, tid_list):
        app = self.app
        if app.replicator.current_connection is not conn:
            return

        if tid_list:
            # If I have pending TIDs, check which TIDs I don't have, and
            # request the data.
            present_tid_list = app.dm.getTIDListPresent(tid_list)
            tid_set = set(tid_list) - set(present_tid_list)
            for tid in tid_set:
                conn.ask(protocol.askTransactionInformation(tid), timeout=300)

            # And, ask more TIDs.
            app.replicator.tid_offset += 1000
            offset = app.replicator.tid_offset
            p = protocol.askTIDs(offset, offset + 1000, 
                      app.replicator.current_partition.getRID())
            conn.ask(p, timeout=300)
        else:
            # If no more TID, a replication of transactions is finished.
            # So start to replicate objects now.
            p = protocol.askOIDs(0, 1000, 
                      app.replicator.current_partition.getRID())
            conn.ask(p, timeout=300)
            app.replicator.oid_offset = 0

    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        app = self.app
        if app.replicator.current_connection is not conn:
            return

        # Directly store the transaction.
        app.dm.storeTransaction(tid, (), (oid_list, user, desc, ext), False)

    def handleAnswerOIDs(self, conn, packet, oid_list):
        app = self.app
        if app.replicator.current_connection is not conn:
            return

        if oid_list:
            # Pick one up, and ask the history.
            oid = oid_list.pop()
            conn.ask(protocol.askObjectHistory(oid, 0, 1000), timeout=300)
            app.replicator.serial_offset = 0
            app.replicator.oid_list = oid_list
        else:
            # Nothing remains, so the replication for this partition is
            # finished.
            app.replicator.replication_done = True
    
    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        app = self.app
        if app.replicator.current_connection is not conn:
            return

        if history_list:
            # Check if I have objects, request those which I don't have.
            serial_list = [t[0] for t in history_list]
            present_serial_list = app.dm.getSerialListPresent(oid, serial_list)
            serial_set = set(serial_list) - set(present_serial_list)
            for serial in serial_set:
                conn.ask(protocol.askObject(oid, serial, None), timeout=300)

            # And, ask more serials.
            app.replicator.serial_offset += 1000
            offset = app.replicator.serial_offset
            p = protocol.askObjectHistory(oid, offset, offset + 1000)
            conn.ask(p, timeout=300)
        else:
            # This OID is finished. So advance to next.
            oid_list = app.replicator.oid_list
            if oid_list:
                # If I have more pending OIDs, pick one up.
                oid = oid_list.pop()
                conn.ask(protocol.askObjectHistory(oid, 0, 1000), timeout=300)
                app.replicator.serial_offset = 0
            else:
                # Otherwise, acquire more OIDs.
                app.replicator.oid_offset += 1000
                offset = app.replicator.oid_offset
                p = protocol.askOIDs(offset, offset + 1000, 
                          app.replicator.current_partition.getRID())
                conn.ask(p, timeout=300)

    def handleAnswerObject(self, conn, packet, oid, serial_start,
                           serial_end, compression, checksum, data):
        app = self.app
        if app.replicator.current_connection is not conn:
            return

        # Directly store the transaction.
        obj = (oid, compression, checksum, data)
        app.dm.storeTransaction(serial_start, [obj], None, False)
        del obj
        del data

