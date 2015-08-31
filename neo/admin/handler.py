#
# Copyright (C) 2009-2019  Nexedi SA
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
from neo.lib.handler import AnswerBaseHandler, EventHandler, MTEventHandler
from neo.lib.pt import PartitionTable
from neo.lib.exception import PrimaryFailure

class MasterBootstrapHandler(EventHandler):

    def connectionFailed(self, conn):
        raise AssertionError

    def connectionClosed(self, conn):
        app = self.app
        try:
            app.__dict__.pop('pt').clear()
        except KeyError:
            pass
        if app.master_conn is not None:
            assert app.master_conn is conn
            raise PrimaryFailure

    def answerClusterState(self, conn, state):
        self.app.cluster_state = state

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        pt = self.app.pt = object.__new__(PartitionTable)
        pt.load(ptid, num_replicas, row_list, self.app.nm)

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        self.app.pt.update(ptid, num_replicas, cell_list, self.app.nm)

    def notifyClusterInformation(self, conn, cluster_state):
        self.app.cluster_state = cluster_state

class MasterEventHandler(MasterBootstrapHandler, MTEventHandler):

    pass

class PrimaryAnswersHandler(AnswerBaseHandler):
    """ This class handle all answer from primary master node"""

    def ack(self, conn, message):
        super(PrimaryAnswersHandler, self).ack(conn, message)
        self.app.setHandlerData(message)

    def denied(self, conn, message):
        raise HTTPError(405, message)

    def protocolError(self, conn, message):
        raise HTTPError(500, message)

    def answerClusterState(self, conn, state):
        self.app.cluster_state = state

    answerRecovery = \
    answerTweakPartitionTable = \
        lambda self, conn, *args: self.app.setHandlerData(args)
