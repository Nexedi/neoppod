#
# Copyright (C) 2009-2015  Nexedi SA
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

import unittest
from mock import Mock
from . import NeoUnitTestBase
from neo.lib.handler import EventHandler
from neo.lib.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError

class HandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        app = Mock()
        self.handler = EventHandler(app)

    def setFakeMethod(self, method):
        self.handler.fake_method = method

    def getFakePacket(self):
        p = Mock({
            'decode': (),
            '__repr__': 'Fake Packet',
        })
        p.handler_method_name = 'fake_method'
        return p

    def test_dispatch(self):
        conn = self.getFakeConnection()
        packet = self.getFakePacket()
        # all is ok
        self.setFakeMethod(lambda c: None)
        self.handler.dispatch(conn, packet)
        # raise UnexpectedPacketError
        conn.mockCalledMethods = {}
        def fake(c):
            raise UnexpectedPacketError('fake packet')
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise PacketMalformedError
        conn.mockCalledMethods = {}
        def fake(c):
            raise PacketMalformedError('message')
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkClosed(conn)
        # raise BrokenNodeDisallowedError
        conn.mockCalledMethods = {}
        def fake(c):
            raise BrokenNodeDisallowedError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise NotReadyError
        conn.mockCalledMethods = {}
        def fake(c):
            raise NotReadyError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise ProtocolError
        conn.mockCalledMethods = {}
        def fake(c):
            raise ProtocolError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)



if __name__ == '__main__':
    unittest.main()
