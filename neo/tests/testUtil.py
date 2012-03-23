#
# Copyright (C) 2006-2011  Nexedi SA

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
from . import NeoUnitTestBase, IP_VERSION_FORMAT_DICT
from neo.lib.util import ReadBuffer, getAddressType, parseNodeAddress, \
    getConnectorFromAddress, SOCKET_CONNECTORS_DICT

class UtilTests(NeoUnitTestBase):

    def test_getConnectorFromAddress(self):
        """ Connector name must correspond to address type """
        connector = getConnectorFromAddress((
                            IP_VERSION_FORMAT_DICT[socket.AF_INET], 0))
        self.assertEqual(connector, SOCKET_CONNECTORS_DICT[socket.AF_INET])
        connector = getConnectorFromAddress((
                            IP_VERSION_FORMAT_DICT[socket.AF_INET6], 0))
        self.assertEqual(connector, SOCKET_CONNECTORS_DICT[socket.AF_INET6])
        self.assertRaises(ValueError, getConnectorFromAddress, ('', 0))
        self.assertRaises(ValueError, getConnectorFromAddress, ('test', 0))

    def test_getAddressType(self):
        """ Get the type on an IP Address """
        self.assertRaises(ValueError, getAddressType, ('', 0))
        address_type = getAddressType(('::1', 0))
        self.assertEqual(address_type, socket.AF_INET6)
        address_type = getAddressType(('0.0.0.0', 0))
        self.assertEqual(address_type, socket.AF_INET)
        address_type = getAddressType(('127.0.0.1', 0))
        self.assertEqual(address_type, socket.AF_INET)

    def _assertEqualParsedAddress(self, parsed, *args, **kw):
        self.assertEqual(parsed, parseNodeAddress(*args, **kw))

    def test_parseNodeAddress(self):
        """ Parsing of addesses """
        test = self._assertEqualParsedAddress
        local_address = socket.gethostbyname('localhost')
        http_port = socket.getservbyname('http')
        test(('127.0.0.1', 0), '127.0.0.1')
        test(('127.0.0.1', 10), '127.0.0.1:10', 500)
        test(('127.0.0.1', 500), '127.0.0.1', 500)
        test(('::1', 0), '[::1]')
        test(('::1', 10), '[::1]:10', 500)
        test(('::1', 500), '[::1]', 500)
        test(('::1', http_port), '[::1]:http')
        test(('::1', 0), '[0::01]')
        test((local_address, 0), 'localhost')
        test((local_address, 10), 'localhost:10')

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

