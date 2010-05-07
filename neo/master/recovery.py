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

from struct import pack

from neo import logging
from neo.util import dump
from neo.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from neo.protocol import NotReadyError
from neo.master.handlers import MasterHandler

REQUIRED_NODE_NUMBER = 1

class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        super(RecoveryManager, self).__init__(app)
        # The target node's uuid to request next.
        self.target_uuid = None

    def getHandler(self):
        return self

    def identifyStorageNode(self, uuid, node):
        """
            Returns the handler for storage nodes
        """
        if uuid is None and not self.app._startup_allowed:
            logging.info('reject empty storage node')
            raise NotReadyError
        return (uuid, NodeStates.RUNNING, self)

    def run(self):
        """
        Recover the status about the cluster. Obtain the last OID, the last
        TID, and the last Partition Table ID from storage nodes, then get
        back the latest partition table or make a new table from scratch,
        if this is the first time.
        """
        logging.info('begin the recovery of the status')

        self.app.changeClusterState(ClusterStates.RECOVERING)
        em = self.app.em

        self.app.loid = None
        self.app.pt.setID(None)

        # collect the last partition table available
        while not self.app._startup_allowed:
            em.poll(1)

        logging.info('startup allowed')

        # build a new partition table
        if self.app.pt.getID() is None:
            self.buildFromScratch()

        # collect node that are connected but not in the selected partition
        # table and set them in pending state
        allowed_node_set = set(self.app.pt.getNodeList())
        refused_node_set = set(self.app.nm.getStorageList()) - allowed_node_set
        for node in refused_node_set:
            node.setPending()
        self.app.broadcastNodesInformation(refused_node_set)

        logging.debug('cluster starts with loid=%s and this partition table :',
                dump(self.app.loid))
        self.app.pt.log()

    def buildFromScratch(self):
        nm, em, pt = self.app.nm, self.app.em, self.app.pt
        logging.debug('creating a new partition table, wait for a storage node')
        # wait for some empty storage nodes, their are accepted
        while len(nm.getStorageList()) < REQUIRED_NODE_NUMBER:
            em.poll(1)
        # take the first node available
        node_list = nm.getStorageList()[:REQUIRED_NODE_NUMBER]
        for node in node_list:
            node.setRunning()
        self.app.broadcastNodesInformation(node_list)
        # resert IDs generators
        self.app.loid = '\0' * 8
        # build the partition with this node
        pt.setID(pack('!Q', 1))
        pt.make(node_list)

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node is not None
        if node.getState() == new_state:
            return
        node.setState(new_state)

    def connectionCompleted(self, conn):
        # XXX: handler split review needed to remove this hack
        if not self.app._startup_allowed:
            # ask the last IDs to perform the recovery
            conn.ask(Packets.AskLastIDs())

    def answerLastIDs(self, conn, loid, ltid, lptid):
        app = self.app
        pt = app.pt

        # Get max values.
        if loid is not None:
            if app.loid is None:
                app.loid = loid
            else:
                app.loid = max(loid, app.loid)
        if ltid is not None:
            self.app.tm.setLastTID(ltid)
        if lptid > pt.getID():
            # something newer
            self.target_uuid = conn.getUUID()
            app.pt.setID(lptid)
            conn.ask(Packets.AskPartitionTable([]))

    def answerPartitionTable(self, conn, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        if uuid != self.target_uuid:
            # If this is not from a target node, ignore it.
            logging.warn('got answer partition table from %s while waiting ' \
                    'for %s', dump(uuid), dump(self.target_uuid))
            return
        # load unknown storage nodes
        new_nodes = []
        for _offset, row in row_list:
            for uuid, _state in row:
                node = app.nm.getByUUID(uuid)
                if node is None:
                    new_nodes.append(app.nm.createStorage(uuid=uuid))
        # notify about new nodes
        if new_nodes:
            self.app.broadcastNodesInformation(new_nodes)
        # load partition in memory
        try:
            self.app.pt.load(ptid, row_list, self.app.nm)
        except IndexError:
            raise ProtocolError('Invalid offset')

