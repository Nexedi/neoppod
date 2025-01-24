#
# Copyright (C) 2006-2019  Nexedi SA

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

import socket
from . import NeoUnitTestBase
from neo.lib.util import parseNodeAddress

class UtilTests(NeoUnitTestBase):

    from neo.storage.shared_queue import test as testSharedQueue

    def test_parseNodeAddress(self):
        """ Parsing of addresses """
        def test(parsed, *args):
            self.assertEqual(parsed, parseNodeAddress(*args))
        http_port = socket.getservbyname('http')
        test((b'127.0.0.1', 0), '127.0.0.1')
        test((b'127.0.0.1', 10), '127.0.0.1:10', 500)
        test((b'127.0.0.1', 500), '127.0.0.1', 500)
        test((b'::1', 0), '[::1]')
        test((b'::1', 10), '[::1]:10', 500)
        test((b'::1', 500), '[::1]', 500)
        test((b'::1', http_port), '[::1]:http')
        test((b'::1', 0), '[0::01]')
        local_address = lambda port: ((b'127.0.0.1', port), (b'::1', port))
        self.assertIn(parseNodeAddress('localhost'), local_address(0))
        self.assertIn(parseNodeAddress('localhost:10'), local_address(10))
