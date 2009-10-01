#
# Copyright (C) 2009  Nexedi SA
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

from neo.handler import EventHandler
from neo.protocol import ErrorCodes

class CommandEventHandler(EventHandler):
    """ Base handler for command """

    def connectionCompleted(self, conn):
        # connected to admin node
        self.app.connected = True
        EventHandler.connectionCompleted(self, conn)

    def __disconnected(self):
        app = self.app
        app.connected = False
        app.connection = None

    def __respond(self, response):
        self.app.response_queue.append(response)

    def connectionClosed(self, conn):
        super(CommandEventHandler, self).connectionClosed(conn)
        self.__disconnected()

    def connectionFailed(self, conn):
        super(CommandEventHandler, self).connectionFailed(conn)
        self.__disconnected()

    def timeoutExpired(self, conn):
        super(CommandEventHandler, self).timeoutExpired(conn)
        self.__disconnected()

    def peerBroken(self, conn):
        super(CommandEventHandler, self).peerBroken(conn)
        self.__disconnected()

    def __handleAnswer(self, conn, packet, *args):
        self.__respond((packet.getType(), ) + args)

    def handleNoError(self, conn, packet, msg):
        self.__respond((packet.getType(), ErrorCodes.NO_ERROR, msg))

    def handleNotReady(self, conn, packet, msg):
        self.__respond((packet.getType(), ErrorCodes.NOT_READY, msg))

    handleAnswerPartitionList = __handleAnswer
    handleAnswerNodeList = __handleAnswer
    handleAnswerNodeState = __handleAnswer
    handleAnswerClusterState = __handleAnswer
    handleAnswerNewNodes = __handleAnswer
    handleAnswerPrimaryMaster = __handleAnswer
