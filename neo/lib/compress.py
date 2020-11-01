#
# Copyright (C) 2018  Nexedi SA
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

import zlib

decompress_list = (
    lambda data: data,
    zlib.decompress,
)

def parseOption(value):
    x = value.split('=', 1)
    try:
        alg = ('zlib',).index(x[0])
        if len(x) == 1:
            return alg, None
        level = int(x[1])
    except Exception:
        raise ValueError("not a valid 'compress' option: %r" % value)
    if 0 < level <= zlib.Z_BEST_COMPRESSION:
        return alg, level
    raise ValueError("invalid compression level: %r" % level)

def getCompress(value):
    if value:
        alg, level = (0, None) if value is True else value
        _compress = zlib.compress
        if level:
            zlib_compress = _compress
            _compress = lambda data: zlib_compress(data, level)
        alg += 1
        assert 0 < alg < len(decompress_list), 'invalid compression algorithm'
        def compress(data):
            size = len(data)
            compressed = _compress(data)
            if len(compressed) < size:
                return size, alg, compressed
            return size, 0, data
        compress._compress = _compress # for testBasicStore
        return compress
    return lambda data: (len(data), 0, data)
