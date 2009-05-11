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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


from zlib import adler32

def dump(s):
    """Dump a binary string in hex."""
    if isinstance(s, str):
        ret = []
        for c in s:
            ret.append('%02x' % ord(c))
        return ''.join(ret)
    else:
        return repr(s)


def bin(s):
    """Inverse of dump method."""
    ret = []
    while len(s):
        ret.append(chr(int(s[:2], 16)))
        s = s[2:]
    return ''.join(ret)
        

def makeChecksum(s):
    """Return a 4-byte integer checksum against a string."""
    return adler32(s) & 0xffffffff
