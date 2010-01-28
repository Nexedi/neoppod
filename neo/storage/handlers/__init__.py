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

from neo.handler import EventHandler
from neo import protocol
from neo.util import dump
from neo.exception import PrimaryFailure, OperationFailure
from neo.protocol import NodeStates, Packets

class BaseStorageHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    pass


class BaseMasterHandler(BaseStorageHandler):

    def connectionLost(self, conn, new_state):
        raise PrimaryFailure('connection lost')

    def reelectPrimary(self, conn, packet):
        raise PrimaryFailure('re-election occurs')

    def notifyClusterInformation(self, conn, packet, state):
        logging.error('ignoring notify cluster information in %s' %
                self.__class__.__name__)

    def notifyLastOID(self, conn, packet, oid):
        self.app.loid = oid
        self.app.dm.setLastOID(oid)

    def notifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if uuid == self.app.uuid:
                # This is me, do what the master tell me
                logging.info("I was told I'm %s" %(state))
                if state in (NodeStates.DOWN, NodeStates.TEMPORARILY_DOWN,
                        NodeStates.BROKEN):
                    conn.close()
                    erase = state == NodeStates.DOWN
                    self.app.shutdown(erase=erase)
                elif state == NodeStates.HIDDEN:
                    raise OperationFailure


class BaseClientAndStorageOperationHandler(BaseStorageHandler):
    """ Accept requests common to client and storage nodes """

    def askTIDs(self, conn, packet, first, last, partition):
        # This method is complicated, because I must return TIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise protocol.ProtocolError('invalid offsets')

        app = self.app
        if partition == protocol.INVALID_PARTITION:
            partition_list = app.pt.getAssignedPartitionList(app.uuid)
        else:
            partition_list = [partition]

        tid_list = app.dm.getTIDList(first, last - first,
                             app.pt.getPartitions(), partition_list)
        conn.answer(Packets.AnswerTIDs(tid_list), packet.getId())

    def askObjectHistory(self, conn, packet, oid, first, last):
        if first >= last:
            raise protocol.ProtocolError( 'invalid offsets')

        app = self.app
        history_list = app.dm.getObjectHistory(oid, first, last - first)
        if history_list is None:
            history_list = []
        p = Packets.AnswerObjectHistory(oid, history_list)
        conn.answer(p, packet.getId())

    def askTransactionInformation(self, conn, packet, tid):
        app = self.app
        t = app.dm.getTransaction(tid)
        if t is None:
            p = protocol.tidNotFound('%s does not exist' % dump(tid))
        else:
            p = Packets.AnswerTransactionInformation(tid, t[1], t[2], t[3],
                    t[0])
        conn.answer(p, packet.getId())

    def askObject(self, conn, packet, oid, serial, tid):
        app = self.app
        if oid in app.load_lock_dict:
            # Delay the response.
            app.queueEvent(self.askObject, conn, packet, oid,
                           serial, tid)
            return
        o = app.dm.getObject(oid, serial, tid)
        if o is not None:
            serial, next_serial, compression, checksum, data = o
            logging.debug('oid = %s, serial = %s, next_serial = %s',
                          dump(oid), dump(serial), dump(next_serial))
            p = Packets.AnswerObject(oid, serial, next_serial,
                           compression, checksum, data)
        else:
            logging.debug('oid = %s not found', dump(oid))
            p = protocol.oidNotFound('%s does not exist' % dump(oid))
        conn.answer(p, packet.getId())

