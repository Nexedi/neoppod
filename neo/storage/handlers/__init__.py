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

import neo

from neo.lib.handler import EventHandler
from neo.lib import protocol
from neo.lib.util import dump
from neo.lib.exception import PrimaryFailure, OperationFailure
from neo.lib.protocol import NodeStates, NodeTypes, Packets, Errors, ZERO_HASH

class BaseMasterHandler(EventHandler):

    def connectionLost(self, conn, new_state):
        if self.app.listening_conn: # if running
            raise PrimaryFailure('connection lost')

    def stopOperation(self, conn):
        raise OperationFailure('operation stopped')

    def reelectPrimary(self, conn):
        raise PrimaryFailure('re-election occurs')

    def notifyClusterInformation(self, conn, state):
        neo.lib.logging.warning('ignoring notify cluster information in %s' %
                self.__class__.__name__)

    def notifyLastOID(self, conn, oid):
        self.app.dm.setLastOID(oid)

    def notifyNodeInformation(self, conn, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if uuid == self.app.uuid:
                # This is me, do what the master tell me
                neo.lib.logging.info("I was told I'm %s" %(state))
                if state in (NodeStates.DOWN, NodeStates.TEMPORARILY_DOWN,
                        NodeStates.BROKEN):
                    erase = state == NodeStates.DOWN
                    self.app.shutdown(erase=erase)
                elif state == NodeStates.HIDDEN:
                    raise OperationFailure
            elif node_type == NodeTypes.CLIENT and state != NodeStates.RUNNING:
                neo.lib.logging.info(
                                'Notified of non-running client, abort (%r)',
                        dump(uuid))
                self.app.tm.abortFor(uuid)


class BaseClientAndStorageOperationHandler(EventHandler):
    """ Accept requests common to client and storage nodes """

    def askTransactionInformation(self, conn, tid):
        app = self.app
        t = app.dm.getTransaction(tid)
        if t is None:
            p = Errors.TidNotFound('%s does not exist' % dump(tid))
        else:
            p = Packets.AnswerTransactionInformation(tid, t[1], t[2], t[3],
                    t[4], t[0])
        conn.answer(p)

    def _askObject(self, oid, serial, tid):
        raise NotImplementedError

    def askObject(self, conn, oid, serial, tid):
        app = self.app
        if self.app.tm.loadLocked(oid):
            # Delay the response.
            app.queueEvent(self.askObject, conn, (oid, serial, tid))
            return
        o = self._askObject(oid, serial, tid)
        if o is None:
            neo.lib.logging.debug('oid = %s does not exist', dump(oid))
            p = Errors.OidDoesNotExist(dump(oid))
        elif o is False:
            neo.lib.logging.debug('oid = %s not found', dump(oid))
            p = Errors.OidNotFound(dump(oid))
        else:
            serial, next_serial, compression, checksum, data, data_serial = o
            neo.lib.logging.debug('oid = %s, serial = %s, next_serial = %s',
                          dump(oid), dump(serial), dump(next_serial))
            if checksum is None:
                checksum = ZERO_HASH
                data = ''
            p = Packets.AnswerObject(oid, serial, next_serial,
                compression, checksum, data, data_serial)
        conn.answer(p)

