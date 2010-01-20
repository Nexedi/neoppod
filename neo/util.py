#
# Copyright (C) 2006-2009  Nexedi SA
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
from zlib import adler32
from struct import pack, unpack

def u64(s):
    return unpack('!Q', s)[0]

def p64(n):
    return pack('!Q', n)

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
    """Return a 4-byte integer checksum against a string."""
    return adler32(s) & 0xffffffff

def parseMasterList(masters, except_node=None):
    if not masters:
        return []
    # load master node list
    master_node_list = []
    for node in masters.split('/'):
        ip_address, port = node.split(':')
        address = (ip_address, int(port))
        if (address != except_node):
            master_node_list.append(address)
    return tuple(master_node_list)


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

