#
# Copyright (C) 2006-2019  Nexedi SA
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
from neo.lib.protocol import NodeTypes, NotReadyError, Packets
from neo.lib.protocol import ProtocolError
from .storage import StorageOperationHandler
from .client import ClientOperationHandler, ClientReadOnlyOperationHandler

class IdentificationHandler(EventHandler):
    """ Handler used for incoming connections during operation state """

    def connectionLost(self, conn, new_state):
        logging.warning('A connection was lost during identification')

    def getEventQueue(self):
        # for requestIdentification
        return self.app.nm

    def requestIdentification(self, conn, node_type, uuid, address, name,
                              id_timestamp, extra):
        self.checkClusterName(name)
        app = self.app
        # reject any incoming connections if not ready
        if not app.operational:
            raise NotReadyError
        if uuid is None:
            if node_type != NodeTypes.STORAGE:
                raise ProtocolError('reject anonymous non-storage node')
            conn.setHandler(StorageOperationHandler(app))
        else:
            if uuid == app.uuid:
                raise ProtocolError("uuid conflict or loopback connection")
            node = app.nm.getByUUID(uuid, id_timestamp)
            # choose the handler according to the node type
            if node_type == NodeTypes.CLIENT:
                if app.dm.getBackupTID():
                    handler = ClientReadOnlyOperationHandler
                else:
                    handler = ClientOperationHandler
                assert node.isRunning(), node
                force = False
            elif node_type == NodeTypes.STORAGE:
                handler = StorageOperationHandler
                force = app.uuid < uuid
            else:
                raise ProtocolError('reject non-client-or-storage node')
            conn.setHandler(handler(app))
            node.setConnection(conn, force)
        # accept the identification and trigger an event
        conn.answer(Packets.AcceptIdentification(
            NodeTypes.STORAGE, uuid and app.uuid, uuid))
