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
from neo.lib.protocol import PacketMalformedError
from neo.lib.util import dump
from neo.lib.handler import EventHandler
from neo.lib.profiling import profiler_decorator

LOGGER_ENABLED = False

class PacketLogger(object):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        _temp = EventHandler(None)
        self.packet_dispatch_table = _temp.packet_dispatch_table
        self.error_dispatch_table = _temp.error_dispatch_table
        self.enable(LOGGER_ENABLED)

    def enable(self, enabled):
        self.dispatch = enabled and self._dispatch or (lambda *args, **kw: None)

    def _dispatch(self, conn, packet, direction):
        """This is a helper method to handle various packet types."""
        # default log message
        klass = packet.getType()
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        packet_name = packet.__class__.__name__
        if packet.isResponse() and packet._request is not None:
            packet_name += packet._request.__name__
        neo.lib.logging.debug('#0x%08x %-30s %s %s (%s:%d)', packet.getId(),
                packet_name, direction, uuid, ip, port)
        # look for custom packet logger
        logger = self.packet_dispatch_table.get(klass, None)
        logger = logger and getattr(self, logger.im_func.__name__, None)
        if logger is None:
            return
        # enhanced log
        try:
            args = packet.decode() or ()
        except PacketMalformedError:
            neo.lib.logging.warning("Can't decode packet for logging")
            return
        log_message = logger(conn, *args)
        if log_message is not None:
            neo.lib.logging.debug('#0x%08x %s', packet.getId(), log_message)

    def error(self, conn, code, message):
        return "%s (%s)" % (code, message)

    def notifyNodeInformation(self, conn, node_list):
        for node_type, address, uuid, state in node_list:
            if address is not None:
                address = '%s:%d' % address
            else:
                address = '?'
            node = (dump(uuid), node_type, address, state)
            neo.lib.logging.debug(' ! %s | %8s | %22s | %s' % node)

PACKET_LOGGER = PacketLogger()
