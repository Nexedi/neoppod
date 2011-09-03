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

from base64 import b64encode
import neo
from neo.lib.protocol import PacketMalformedError
from neo.lib.util import dump
from neo.lib.handler import EventHandler
from neo.lib.profiling import profiler_decorator

LOGGER_ENABLED = False

class PacketLogger(object):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        self.enable(LOGGER_ENABLED)

    def enable(self, enabled):
        self.dispatch = enabled and self._dispatch or (lambda *args, **kw: None)

    def _dispatch(self, conn, packet, outgoing):
        """This is a helper method to handle various packet types."""
        # default log message
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        packet_name = packet.__class__.__name__
        neo.lib.logging.debug('#0x%04x %-30s %s %s (%s:%d) %s', packet.getId(),
                packet_name, outgoing and '>' or '<', uuid, ip, port,
                b64encode(packet._body[:96]))
        # look for custom packet logger
        logger = getattr(self, packet.handler_method_name, None)
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
            neo.lib.logging.debug('#0x%04x %s', packet.getId(), log_message)

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
