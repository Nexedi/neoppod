#
# Copyright (C) 2006-2015  Nexedi SA

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

import unittest
import socket
from . import NeoUnitTestBase
from neo.lib.util import ReadBuffer, parseNodeAddress

class UtilTests(NeoUnitTestBase):

    def test_parseNodeAddress(self):
        """ Parsing of addesses """
        def test(parsed, *args):
            self.assertEqual(parsed, parseNodeAddress(*args))
        http_port = socket.getservbyname('http')
        test(('127.0.0.1', 0), '127.0.0.1')
        test(('127.0.0.1', 10), '127.0.0.1:10', 500)
        test(('127.0.0.1', 500), '127.0.0.1', 500)
        test(('::1', 0), '[::1]')
        test(('::1', 10), '[::1]:10', 500)
        test(('::1', 500), '[::1]', 500)
        test(('::1', http_port), '[::1]:http')
        test(('::1', 0), '[0::01]')
        local_address = lambda port: (('127.0.0.1', port), ('::1', port))
        self.assertIn(parseNodeAddress('localhost'), local_address(0))
        self.assertIn(parseNodeAddress('localhost:10'), local_address(10))

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

