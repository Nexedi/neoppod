#
# Copyright (C) 2006-2015  Nexedi SA
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
from neo.lib.handler import EventHandler
from neo.lib.exception import PrimaryFailure, OperationFailure
from neo.lib.protocol import uuid_str, NodeStates, NodeTypes

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
        self.app.changeClusterState(state)

    def notifyNodeInformation(self, conn, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if uuid == self.app.uuid:
                # This is me, do what the master tell me
                logging.info("I was told I'm %s", state)
                if state in (NodeStates.DOWN, NodeStates.TEMPORARILY_DOWN,
                        NodeStates.BROKEN, NodeStates.UNKNOWN):
                    erase = state == NodeStates.DOWN
                    self.app.shutdown(erase=erase)
                elif state == NodeStates.HIDDEN:
                    raise OperationFailure
            elif node_type == NodeTypes.CLIENT and state != NodeStates.RUNNING:
                logging.info('Notified of non-running client, abort (%s)',
                        uuid_str(uuid))
                self.app.tm.abortFor(uuid)

    def answerUnfinishedTransactions(self, conn, *args, **kw):
        self.app.replicator.setUnfinishedTIDList(*args, **kw)
