# -*- coding: utf-8 -*-
# Copyright (C) 2018  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
"""NEO/py tracing equivalent to one in NEO/go

This module activates tracepoints equivalent to NEO/go on NEO/py side.

Must be used with pyruntraced.
"""

from neo.lib.connection import Connection
import socket

@trace_entry(Connection._addPacket, 'MsgSendPre')
def _(self, packet):
    sk = self.connector.socket
    pkth, data = packet.encode()
    return {'src':  sk_localaddr(sk),
            'dst':  sk_remoteaddr(sk),
            # XXX pkt goes as quoted to avoid UTF-8 decode problem on json.dump
            'pktq': `pkth+data`}
            #'pktq': (pkth+data).encode('hex')}


# sk_addr converts socket address tuple for family to string
def sk_addr(addr, family):
    if family == socket.AF_INET:
        host, port = addr
        return '%s:%s' % (host, port)

    else:
        raise RuntimError('sk_addr: TODO: %s' % family)

def sk_localaddr(sk):
    return sk_addr( sk.getsockname(), sk.family )

def sk_remoteaddr(sk):
    return sk_addr( sk.getpeername(), sk.family )