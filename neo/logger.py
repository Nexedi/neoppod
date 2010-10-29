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

import neo
from neo.protocol import PacketMalformedError
from neo.util import dump
from neo.handler import EventHandler
from neo.profiling import profiler_decorator

LOGGER_ENABLED = True

class PacketLogger(object):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        _temp = EventHandler(None)
        self.packet_dispatch_table = _temp.packet_dispatch_table
        self.error_dispatch_table = _temp.error_dispatch_table

    def dispatch(self, conn, packet, direction):
        """This is a helper method to handle various packet types."""
        # default log message
        klass = packet.getType()
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        neo.logging.debug('#0x%08x %-30s %s %s (%s:%d)', packet.getId(),
                packet.__class__.__name__, direction, uuid, ip, port)
        # look for custom packet logger
        logger = self.packet_dispatch_table.get(klass, None)
        logger = logger and getattr(self, logger.im_func.__name__, None)
        if logger is None:
            return
        # enhanced log
        try:
            args = packet.decode() or ()
        except PacketMalformedError:
            neo.logging.warning("Can't decode packet for logging")
            return
        log_message = logger(conn, *args)
        if log_message is not None:
            neo.logging.debug('#0x%08x %s', packet.getId(), log_message)

    def error(self, conn, code, message):
        return "%s (%s)" % (code, message)

    def notifyNodeInformation(self, conn, node_list):
        for node_type, address, uuid, state in node_list:
            if address is not None:
                address = '%s:%d' % address
            else:
                address = '?'
            node = (dump(uuid), node_type, address, state)
            neo.logging.debug(' ! %s | %8s | %22s | %s' % node)

PACKET_LOGGER = PacketLogger()
if not LOGGER_ENABLED:
    # disable logger
    PACKET_LOGGER.dispatch = lambda *args, **kw: None
