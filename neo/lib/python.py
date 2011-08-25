# Copyright (C) 2011  Nexedi SA
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

import sys, types

if sys.version_info < (2, 5):
    import __builtin__, imp

    def all(iterable):
      """
      Return True if bool(x) is True for all values x in the iterable.
      """
      for x in iterable:
        if not x:
          return False
      return True
    __builtin__.all = all

    def any(iterable):
      """
      Return True if bool(x) is True for any x in the iterable.
      """
      for x in iterable:
        if x:
          return True
      return False
    __builtin__.any = any

    import md5, sha
    sys.modules['hashlib'] = hashlib = imp.new_module('hashlib')
    hashlib.md5 = md5.new
    hashlib.sha1 = sha.new

    import struct

    class Struct(object):

        def __init__(self, fmt):
            self._fmt = fmt
            self.size = struct.calcsize(fmt)

        def pack(self, *args):
            return struct.pack(self._fmt, *args)

        def unpack(self, *args):
            return struct.unpack(self._fmt, *args)

    struct.Struct = Struct

    sys.modules['functools'] = functools = imp.new_module('functools')

    def wraps(wrapped):
        """Simple backport of functools.wraps from Python >= 2.5"""
        def decorator(wrapper):
            wrapper.__module__ = wrapped.__module__
            wrapper.__name__ = wrapped.__name__
            wrapper.__doc__ = wrapped.__doc__
            wrapper.__dict__.update(wrapped.__dict__)
            return wrapper
        return decorator

    functools.wraps = wraps
