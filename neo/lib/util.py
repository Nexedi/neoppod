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


import re
import socket
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
    if s is None:
        return None
    if isinstance(s, str):
        ret = []
        for c in s:
            ret.append('%02x' % ord(c))
        return ''.join(ret)
    else:
        return repr(s)


def bin(s):
    """Inverse of dump method."""
    if s is None:
        return None
    ret = []
    while len(s):
        ret.append(chr(int(s[:2], 16)))
        s = s[2:]
    return ''.join(ret)


def makeChecksum(s):
    """Return a 20-byte checksum against a string."""
    return sha1(s).digest()


def resolve(hostname):
    """
        Returns the first IP address that match with the given hostname
    """
    try:
        # an IP resolves to itself
        _, _, address_list = socket.gethostbyname_ex(hostname)
    except socket.gaierror:
        return None
    return address_list[0]

def getAddressType(address):
    "Return the type (IPv4 or IPv6) of an ip"
    (host, port) = address

    for af_type in SOCKET_CONNECTORS_DICT.keys():
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
    if ']' in address:
       (ip, port) = address.split(']')
       ip = ip.lstrip('[')
       port = port.lstrip(':')
       if port == '':
           port = port_opt
    elif ':' in address:
        (ip, port) = address.split(':')
        ip = resolve(ip)
    else:
        ip = address
        port = port_opt

    if port is None:
        raise ValueError
    return (ip, int(port))

def parseMasterList(masters, except_node=None):
    assert masters, 'At least one master must be defined'
    socket_connector = ''
    # load master node list
    master_node_list = []
    # XXX: support '/' and ' ' as separator
    masters = masters.replace('/', ' ')
    for node in masters.split(' '):
        address = parseNodeAddress(node)

        if (address != except_node):
            master_node_list.append(address)

        socket_connector_temp = getConnectorFromAddress(address)
        if socket_connector == '':
            socket_connector = socket_connector_temp
        elif socket_connector == socket_connector_temp:
           pass
        else:
            return TypeError, (" Wrong connector type : you're trying to use ipv6 and ipv4 simultaneously")

    return tuple(master_node_list), socket_connector

class Enum(dict):
    """
    Simulate an enumeration, define them as follow :
        class MyEnum(Enum):
          ITEM1 = Enum.Item(0)
          ITEM2 = Enum.Item(1)
    Enum items must be written in full upper case
    """

    class Item(int):

        _enum = None
        _name = None

        def __new__(cls, value):
            instance = super(Enum.Item, cls).__new__(cls, value)
            instance._enum = None
            instance._name = None
            return instance

        def __str__(self):
            return self._name

        def __repr__(self):
            return "<EnumItem %s (%d)>" % (self._name, self)

        def __eq__(self, other):
            if other is None:
                return False
            assert isinstance(other, (Enum.Item, int, float, long))
            if isinstance(other, Enum):
                assert self._enum == other._enum
            return int(self) == int(other)

    def __init__(self):
        dict.__init__(self)
        for name in dir(self):
            if not re.match('^[A-Z_]*$', name):
                continue
            item = getattr(self, name)
            item._name = name
            item._enum = self
            self[int(item)] = item

    def getByName(self, name):
        return getattr(self, name)


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
        chunk_len = 0
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

