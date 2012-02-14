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
from neo.lib.util import dump
from neo.lib.exception import PrimaryFailure, OperationFailure
from neo.lib.protocol import NodeStates, NodeTypes

class BaseMasterHandler(EventHandler):

    def connectionLost(self, conn, new_state):
        if self.app.listening_conn: # if running
            self.app.master_node = None
            raise PrimaryFailure('connection lost')

    def stopOperation(self, conn):
        raise OperationFailure('operation stopped')

    def reelectPrimary(self, conn):
        raise PrimaryFailure('re-election occurs')

    def notifyClusterInformation(self, conn, state):
        neo.lib.logging.warning('ignoring notify cluster information in %s',
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
                neo.lib.logging.info("I was told I'm %s", state)
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

    def answerUnfinishedTransactions(self, conn, *args, **kw):
        self.app.replicator.setUnfinishedTIDList(*args, **kw)
