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
from struct import pack, unpack
from time import time, gmtime

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

def getNextTID(ltid):
    """ Compute the next TID based on the current time and check collisions """
    tm = time()
    gmt = gmtime(tm)
    upper = ((((gmt.tm_year - 1900) * 12 + gmt.tm_mon - 1) * 31 \
              + gmt.tm_mday - 1) * 24 + gmt.tm_hour) * 60 + gmt.tm_min
    lower = int((gmt.tm_sec % 60 + (tm - int(tm))) / (60.0 / 65536.0 / 65536.0))
    tid = pack('!LL', upper, lower)
    if tid <= ltid:
        upper, lower = unpack('!LL', ltid)
        if lower == 0xffffffff:
            # This should not happen usually.
            from datetime import timedelta, datetime
            d = datetime(gmt.tm_year, gmt.tm_mon, gmt.tm_mday, 
                         gmt.tm_hour, gmt.tm_min) \
                    + timedelta(0, 60)
            upper = ((((d.year - 1900) * 12 + d.month - 1) * 31 \
                      + d.day - 1) * 24 + d.hour) * 60 + d.minute
            lower = 0
        else:
            lower += 1
        tid = pack('!LL', upper, lower)
    return tid
