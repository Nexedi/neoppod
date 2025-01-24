#
# Copyright (C) 2006-2019  Nexedi SA
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

from __future__ import division
import os, socket
from binascii import a2b_hex, b2a_hex
from collections import deque
from datetime import timedelta, datetime
from hashlib import sha1
from struct import pack, unpack, Struct
from time import gmtime
from neo import *

# https://stackoverflow.com/a/6163157
def nextafter():
    global nextafter
    from ctypes import CDLL, util as ctypes_util, c_double
    from time import time
    _libm = CDLL(ctypes_util.find_library('m'))
    nextafter = _libm.nextafter
    nextafter.restype = c_double
    nextafter.argtypes = c_double, c_double
    x = time()
    y = nextafter(x, float('inf'))
    assert x < y and (x+y)/2 in (x,y), (x, y)
nextafter()

TID_LOW_OVERFLOW = 2**32
TID_LOW_MAX = TID_LOW_OVERFLOW - 1
SECOND_FROM_UINT32 = 60 / TID_LOW_OVERFLOW
MICRO_FROM_UINT32 = 1e6 / TID_LOW_OVERFLOW
TID_CHUNK_RULES = (
    (-1900, 0),
    (-1, 12),
    (-1, 31),
    (0, 24),
    (0, 60),
)

def tidFromTime(tm):
    gmt = gmtime(tm)
    return packTID(
        (gmt.tm_year, gmt.tm_mon, gmt.tm_mday, gmt.tm_hour, gmt.tm_min),
        int((gmt.tm_sec + (tm - int(tm))) / SECOND_FROM_UINT32))

def packTID(higher, lower):
    """
    higher: a 5-tuple containing year, month, day, hour and minute
    lower: seconds scaled to 60:2**32 into a 64 bits TID
    """
    assert len(higher) == len(TID_CHUNK_RULES), higher
    packed_higher = 0
    for value, (offset, multiplicator) in zip(higher, TID_CHUNK_RULES):
        assert isinstance(value, six.integer_types), value
        value += offset
        assert 0 <= value, (value, offset, multiplicator)
        assert multiplicator == 0 or value < multiplicator, (value,
            offset, multiplicator)
        packed_higher *= multiplicator
        packed_higher += value
    # If the machine is configured in such way that gmtime() returns leap
    # seconds (e.g. TZ=right/UTC), then the best we can do is to use
    # TID_LOW_MAX, because TID format was not designed to support them.
    # For more information about leap seconds on Unix, see:
    #   https://en.wikipedia.org/wiki/Unix_time
    #   http://www.madore.org/~david/computers/unix-leap-seconds.html
    return pack('!LL', packed_higher, min(lower, TID_LOW_MAX))

def unpackTID(ptid):
    """
    Unpack given 64 bits TID in to a 2-tuple containing:
    - a 5-tuple containing year, month, day, hour and minute
    - seconds scaled to 60:2**32
    """
    packed_higher, lower = unpack('!LL', ptid)
    higher = []
    append = higher.append
    for offset, multiplicator in reversed(TID_CHUNK_RULES):
        if multiplicator:
            packed_higher, value = divmod(packed_higher, multiplicator)
        else:
            packed_higher, value = 0, packed_higher
        append(value - offset)
    higher.reverse()
    return (tuple(higher), lower)

def datetimeFromTID(tid):
    higher, lower = unpackTID(tid)
    seconds, lower = divmod(lower * 60, TID_LOW_OVERFLOW)
    return datetime(*(higher + (seconds, int(lower * MICRO_FROM_UINT32))))

def timeFromTID(tid, _epoch=datetime.utcfromtimestamp(0)):
    return (datetimeFromTID(tid) - _epoch).total_seconds()

def addTID(ptid, offset):
    """
    Offset given packed TID.
    """
    higher, lower = unpackTID(ptid)
    high_offset, lower = divmod(lower + offset, TID_LOW_OVERFLOW)
    if high_offset:
        d = datetime(*higher) + timedelta(0, 60 * high_offset)
        higher = (d.year, d.month, d.day, d.hour, d.minute)
    return packTID(higher, lower)

p64, u64 = (lambda unpack: (
    unpack.__self__.pack,
    lambda s: unpack(s)[0]
))(Struct('!Q').unpack)

def add64(packed, offset):
    """Add a python number to a 64-bits packed value"""
    return p64(u64(packed) + offset)

def dump(s):
    """Dump a binary string in hex."""
    if s is not None:
        if isinstance(s, bytes):
            return bytes2str(b2a_hex(s))
        return repr(s)

def bin(s):
    """Inverse of dump method."""
    if s is not None:
        return a2b_hex(s)


def makeChecksum(s):
    """Return a 20-byte checksum against a string."""
    return sha1(s).digest()


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
    # Resolve (maybe) and cast to canonical form
    # XXX: Always pick the first result. This might not be what is desired, and
    # if so this function should either take a hint on the desired address type
    # or return either raw host & port or getaddrinfo return value.
    return encodeAddress(socket.getaddrinfo(
        host, port, 0, socket.SOCK_STREAM)[0][4][:2])

def parseMasterList(masters):
    return list(map(parseNodeAddress, masters.split()))


class cached_property(object):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    """

    def __init__(self, func):
        self.__doc__ = func.__doc__
        self.func = func

    def __get__(self, obj, cls):
        if obj is None: return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value

# This module is always imported before multiprocessing is used, and the
# main process does not want to change name when task are run in threads.
spt_pid = os.getpid()

def setproctitle(title):
    global spt_pid
    pid = os.getpid()
    if spt_pid == pid:
        return
    spt_pid = pid
    # Try using https://pypi.org/project/setproctitle/
    try:
        # On Linux, this is done by clobbering argv, and the main process
        # usually has a longer command line than the title of subprocesses.
        os.environ['SPT_NOENV'] = '1'
        from setproctitle import setproctitle
    except ImportError:
        return
    finally:
        del os.environ['SPT_NOENV']
    setproctitle(title)
