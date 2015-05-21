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

from neo.lib.handler import EventHandler
from neo.lib.protocol import ErrorCodes, Packets

class CommandEventHandler(EventHandler):
    """ Base handler for command """

    def connectionCompleted(self, conn):
        # connected to admin node
        self.app.connected = True
        super(CommandEventHandler, self).connectionCompleted(conn)

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

    def ack(self, conn, msg):
        self.__respond((Packets.Error, ErrorCodes.ACK, msg))

    def protocolError(self, conn, msg):
        self.__respond((Packets.Error, ErrorCodes.PROTOCOL_ERROR, msg))

    def notReady(self, conn, msg):
        self.__respond((Packets.Error, ErrorCodes.NOT_READY, msg))

    def __answer(packet_type):
        def answer(self, conn, *args):
            self.__respond((packet_type, ) + args)
        return answer

    answerPartitionList = __answer(Packets.AnswerPartitionList)
    answerNodeList = __answer(Packets.AnswerNodeList)
    answerClusterState = __answer(Packets.AnswerClusterState)
    answerPrimary = __answer(Packets.AnswerPrimary)
    answerLastIDs = __answer(Packets.AnswerLastIDs)
    answerLastTransaction = __answer(Packets.AnswerLastTransaction)
