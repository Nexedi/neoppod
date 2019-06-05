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
from neo.lib.exception import PrimaryElected
from neo.lib.handler import EventHandler
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, \
    NodeTypes, NotReadyError, Packets, ProtocolError, uuid_str
from ..app import monotonic_time

class IdentificationHandler(EventHandler):

    def requestIdentification(self, conn, node_type, uuid,
                              address, name, id_timestamp, extra):
        app = self.app
        self.checkClusterName(name)
        if address == app.server:
            raise ProtocolError('address conflict')
        node = app.nm.getByUUID(uuid)
        by_addr = address and app.nm.getByAddress(address)
        while 1:
            if by_addr:
                if not by_addr.isIdentified():
                    if node is by_addr:
                        break
                    if not node or uuid < 0:
                        # In case of address conflict for a peer with temporary
                        # ids, we'll generate a new id.
                        node = by_addr
                        break
            elif node:
                if node.isIdentified():
                    if uuid < 0:
                        # The peer wants a temporary id that's already assigned.
                        # Let's give it another one.
                        node = uuid = None
                        break
                else:
                    if node is app._node:
                        node = None
                    else:
                        node.setAddress(address)
                    break
                # Id conflict for a storage node.
            else:
                break
            # cloned/evil/buggy node connecting to us
            raise ProtocolError('already connected')

        new_nid = extra.pop('new_nid', None)
        state = NodeStates.RUNNING
        if node_type == NodeTypes.CLIENT:
            if app.cluster_state == ClusterStates.RUNNING:
                handler = app.client_service_handler
            elif app.cluster_state == ClusterStates.BACKINGUP:
                handler = app.client_ro_service_handler
            else:
                raise NotReadyError
            human_readable_node_type = ' client '
        elif node_type == NodeTypes.STORAGE:
            if app.cluster_state == ClusterStates.STOPPING_BACKUP:
                raise NotReadyError
            manager = app._current_manager
            if manager is None:
                manager = app
            state, handler = manager.identifyStorageNode(
                uuid is not None and node is not None)
            if not address:
                if app.cluster_state == ClusterStates.RECOVERING:
                    raise NotReadyError
                if uuid or not new_nid:
                    raise ProtocolError
                state = NodeStates.DOWN
                # We'll let the storage node close the connection. If we
                # aborted it at the end of the method, BootstrapManager
                # (which is used by storage nodes) could see the closure
                # and try to reconnect to a master.
            human_readable_node_type = ' storage (%s) ' % (state, )
        elif node_type == NodeTypes.MASTER:
            if app.election:
                if id_timestamp and \
                  (id_timestamp, address) < (app.election, app.server):
                    raise PrimaryElected(by_addr or
                        app.nm.createMaster(address=address))
                handler = app.election_handler
            else:
                handler = app.secondary_handler
            human_readable_node_type = ' master '
        elif node_type == NodeTypes.ADMIN:
            handler = app.administration_handler
            human_readable_node_type = 'n admin '
        else:
            raise ProtocolError

        uuid = app.getNewUUID(uuid, address, node_type)
        logging.info('Accept a' + human_readable_node_type + uuid_str(uuid))
        if node is None:
            node = app.nm.createFromNodeType(node_type,
                uuid=uuid, address=address)
        else:
            node.setUUID(uuid)
        node.extra = extra
        node.id_timestamp = monotonic_time()
        node.setState(state)
        app.broadcastNodesInformation([node])
        if new_nid:
            changed_list = []
            for offset in new_nid:
                changed_list.append((offset, uuid, CellStates.OUT_OF_DATE))
                app.pt._setCell(offset, node, CellStates.OUT_OF_DATE)
            app.broadcastPartitionChanges(changed_list)
        conn.setHandler(handler)
        node.setConnection(conn, not node.isIdentified())

        conn.answer(Packets.AcceptIdentification(
            NodeTypes.MASTER,
            app.uuid,
            uuid))
        handler._notifyNodeInformation(conn)
        handler.handlerSwitched(conn, True)

class SecondaryIdentificationHandler(EventHandler):

    def requestIdentification(self, conn, node_type, uuid,
                              address, name, id_timestamp, extra):
        app = self.app
        self.checkClusterName(name)
        if address == app.server:
            raise ProtocolError('address conflict')
        primary = app.primary_master.getAddress()
        if primary == address:
            primary = None
        elif not app.primary_master.isIdentified():
            if node_type == NodeTypes.MASTER:
                node = app.nm.createMaster(address=address)
                if id_timestamp:
                    conn.close()
                    raise PrimaryElected(node)
            primary = None
        # For some cases, we rely on the fact that the remote will not retry
        # immediately (see SocketConnector.CONNECT_LIMIT).
        known_master_list = [node.getAddress()
            for node in app.nm.getMasterList()]
        conn.send(Packets.NotPrimaryMaster(
            primary and known_master_list.index(primary),
            known_master_list))
        conn.abort()
