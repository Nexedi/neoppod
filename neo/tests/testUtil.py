#
# Copyright (C) 2006-2010  Nexedi SA

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

import unittest

from neo.tests import NeoTestBase
from neo.util import ReadBuffer

class UtilTests(NeoTestBase):

    def testReadBufferRead(self):
        """ Append some chunk then consume the data """
        buf = ReadBuffer()
        self.assertEqual(len(buf), 0)
        buf.append('abc')
        self.assertEqual(len(buf), 3)
        # no enough data
        self.assertEqual(buf.read(4), None)
        self.assertEqual(len(buf), 3)
        buf.append('def')
        # consume a part
        self.assertEqual(len(buf), 6)
        self.assertEqual(buf.read(4), 'abcd')
        self.assertEqual(len(buf), 2)
        # consume the rest
        self.assertEqual(buf.read(3), None)
        self.assertEqual(buf.read(2), 'ef')

if __name__ == "__main__":
    unittest.main()

