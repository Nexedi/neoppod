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

import neo.lib

from neo.lib.handler import EventHandler
from neo.lib.protocol import NodeTypes, Packets, NotReadyError
from neo.lib.protocol import ProtocolError, BrokenNodeDisallowedError
from neo.lib.util import dump

class IdentificationHandler(EventHandler):
    """ Handler used for incoming connections during operation state """

    def connectionLost(self, conn, new_state):
        neo.lib.logging.warning('A connection was lost during identification')

    def requestIdentification(self, conn, node_type,
                                        uuid, address, name):
        self.checkClusterName(name)
        # reject any incoming connections if not ready
        if not self.app.ready:
            raise NotReadyError
        app = self.app
        node = app.nm.getByUUID(uuid)
        # If this node is broken, reject it.
        if node is not None and node.isBroken():
            raise BrokenNodeDisallowedError
        # choose the handler according to the node type
        if node_type == NodeTypes.CLIENT:
            from neo.storage.handlers.client import ClientOperationHandler
            handler = ClientOperationHandler
            if node is None:
                node = app.nm.createClient()
            elif node.isConnected():
                # cut previous connection
                node.getConnection().close()
                assert not node.isConnected()
            node.setRunning()
        elif node_type == NodeTypes.STORAGE:
            from neo.storage.handlers.storage import StorageOperationHandler
            handler = StorageOperationHandler
            if node is None:
                neo.lib.logging.error('reject an unknown storage node %s',
                    dump(uuid))
                raise NotReadyError
        else:
            raise ProtocolError('reject non-client-or-storage node')
        # apply the handler and set up the connection
        handler = handler(self.app)
        conn.setUUID(uuid)
        conn.setHandler(handler)
        node.setUUID(uuid)
        node.setConnection(conn)
        args = (NodeTypes.STORAGE, app.uuid, app.pt.getPartitions(),
            app.pt.getReplicas(), uuid)
        # accept the identification and trigger an event
        conn.answer(Packets.AcceptIdentification(*args))
        handler.connectionCompleted(conn)

