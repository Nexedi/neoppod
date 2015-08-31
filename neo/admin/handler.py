#
# Copyright (C) 2009-2015  Nexedi SA
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

from bottle import HTTPError
from neo.lib import logging
from neo.lib.handler import AnswerBaseHandler, EventHandler, MTEventHandler
from neo.lib.exception import PrimaryFailure

class MasterEventHandler(EventHandler):

    def _connectionLost(self, conn):
        conn.cancelRequests("connection to master lost")
        self.app.nm.getByUUID(conn.getUUID()).setUnknown()
        if self.app.master_conn is not None:
            assert self.app.master_conn is conn
            raise PrimaryFailure

    def connectionFailed(self, conn):
        self._connectionLost(conn)

    def connectionClosed(self, conn):
        self._connectionLost(conn)

    def answerClusterState(self, conn, state):
        self.app.cluster_state = state

    def answerNodeInformation(self, conn):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("answerNodeInformation")

    def notifyNodeInformation(self, conn, node_list):
        self.app.nm.update(node_list)

    def answerPartitionTable(self, conn, ptid, row_list):
        self.app.pt.load(ptid, row_list, self.app.nm)

class MasterNotificationsHandler(MasterEventHandler, MTEventHandler):

    notifyClusterInformation = MasterEventHandler.answerClusterState.im_func
    sendPartitionTable = MasterEventHandler.answerPartitionTable.im_func

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        self.app.pt.update(ptid, cell_list, self.app.nm)

class PrimaryAnswersHandler(AnswerBaseHandler):
    """ This class handle all answer from primary master node"""

    def protocolError(self, conn, message):
        raise HTTPError(400, message)
