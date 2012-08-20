#
# Copyright (C) 2006-2012  Nexedi SA
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


import re
import socket
from binascii import a2b_hex, b2a_hex
from hashlib import sha1
from Queue import deque
from struct import pack, unpack

SOCKET_CONNECTORS_DICT = {
    socket.AF_INET : 'SocketConnectorIPv4',
    socket.AF_INET6: 'SocketConnectorIPv6',
}

def u64(s):
    return unpack('!Q', s)[0]

def p64(n):
    return pack('!Q', n)

def add64(packed, offset):
    """Add a python number to a 64-bits packed value"""
    return p64(u64(packed) + offset)

def dump(s):
    """Dump a binary string in hex."""
    if s is not None:
        if isinstance(s, str):
            return b2a_hex(s)
        return repr(s)

def bin(s):
    """Inverse of dump method."""
    if s is not None:
        return a2b_hex(s)


def makeChecksum(s):
    """Return a 20-byte checksum against a string."""
    return sha1(s).digest()


def getAddressType(address):
    "Return the type (IPv4 or IPv6) of an ip"
    (host, port) = address

    for af_type in SOCKET_CONNECTORS_DICT:
        try :
            socket.inet_pton(af_type, host)
        except:
            continue
        else:
            break
    else:
        raise ValueError("Unknown type of host", host)
    return af_type

def getConnectorFromAddress(address):
    address_type = getAddressType(address)
    return SOCKET_CONNECTORS_DICT[address_type]

def parseNodeAddress(address, port_opt=None):
    if address[:1] == '[':
       (host, port) = address[1:].split(']')
       if port[:1] == ':':
           port = port[1:]
       else:
           port = port_opt
    elif address.count(':') == 1:
        (host, port) = address.split(':')
    else:
        host = address
        port = port_opt
    # Resolve (maybe) and cast to cannonical form
    # XXX: Always pick the first result. This might not be what is desired, and
    # if so this function should either take a hint on the desired address type
    # or return either raw host & port or getaddrinfo return value.
    return socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)[0][4][:2]

def parseMasterList(masters, except_node=None):
    assert masters, 'At least one master must be defined'
    # load master node list
    socket_connector = None
    master_node_list = []
    for node in masters.split(' '):
        if not node:
            continue
        address = parseNodeAddress(node)

        if (address != except_node):
            master_node_list.append(address)

        socket_connector_temp = getConnectorFromAddress(address)
        if socket_connector is None:
            socket_connector = socket_connector_temp
        elif socket_connector != socket_connector_temp:
            raise TypeError("Wrong connector type : you're trying to use "
                "ipv6 and ipv4 simultaneously")
    return master_node_list, socket_connector


class ReadBuffer(object):
    """
        Implementation of a lazy buffer. Main purpose if to reduce useless
        copies of data by storing chunks and join them only when the requested
        size is available.
    """

    def __init__(self):
        self.size = 0
        self.content = deque()

    def append(self, data):
        """ Append some data and compute the new buffer size """
        size = len(data)
        self.size += size
        self.content.append((size, data))

    def __len__(self):
        """ Return the current buffer size """
        return self.size

    def read(self, size):
        """ Read and consume size bytes """
        if self.size < size:
            return None
        self.size -= size
        chunk_list = []
        pop_chunk = self.content.popleft
        append_data = chunk_list.append
        to_read = size
        # select required chunks
        while to_read > 0:
            chunk_size, chunk_data = pop_chunk()
            to_read -= chunk_size
            append_data(chunk_data)
        if to_read < 0:
            # too many bytes consumed, cut the last chunk
            last_chunk = chunk_list[-1]
            keep, let = last_chunk[:to_read], last_chunk[to_read:]
            self.content.appendleft((-to_read, let))
            chunk_list[-1] = keep
        # join all chunks (one copy)
        data = ''.join(chunk_list)
        assert len(data) == size
        return data

    def clear(self):
        """ Erase all buffer content """
        self.size = 0
        self.content.clear()

