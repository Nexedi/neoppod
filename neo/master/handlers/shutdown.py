#
# Copyright (C) 2006-2009  Nexedi SA

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

import logging
from neo import protocol
from neo.protocol import CLIENT_NODE_TYPE
from neo.master.handlers import BaseServiceHandler
from neo import decorators

class ShutdownHandler(BaseServiceHandler):
    """This class deals with events for a shutting down phase."""

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        logging.error('reject any new connection')
        raise protocol.ProtocolError('cluster is shutting down')


    def handleAskPrimaryMaster(self, conn, packet):
        logging.error('reject any new demand for primary master')
        raise protocol.ProtocolError('cluster is shutting down')

    @decorators.identification_required
    @decorators.restrict_node_types(CLIENT_NODE_TYPE)
    def handleAskBeginTransaction(self, conn, packet):
        logging.error('reject any new demand for new tid')
        raise protocol.ProtocolError('cluster is shutting down')

    @decorators.identification_required
    def handleNotifyNodeInformation(self, conn, packet, node_list):
        # don't care about notifications since we are shutdowning
        pass
