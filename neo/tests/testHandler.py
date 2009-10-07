#
# Copyright (C) 2009  Nexedi SA
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

import unittest
from mock import Mock
from neo.tests import NeoTestBase
from neo.handler import EventHandler
from neo.protocol import UnexpectedPacketError
from neo.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError

class HandlerTests(NeoTestBase):

    def setUp(self):
        app = Mock()
        self.handler = EventHandler(app)
        self.fake_type = 'FAKE_PACKET_TYPE'

    def setFakeMethod(self, method):
        self.handler.packet_dispatch_table[self.fake_type] = method

    def getFakePacket(self):
        return Mock({'getType': self.fake_type, 'decode': ()})

    def checkFakeCalled(self):
        method = self.handler.packet_dispatch_table[self.fake_type]
        calls = method.getNamedCalls('__call__')
        self.assertEquals(len(calls), 1)

    def test_dispatch(self):
        conn = Mock({'getAddress': ('127.0.0.1', 10000)})
        packet = self.getFakePacket()
        # all is ok
        self.setFakeMethod(lambda c, p: None)
        self.handler.dispatch(conn, packet)
        # raise UnexpectedPacketError 
        conn.mockCalledMethods = {} 
        def fake(c, p): raise UnexpectedPacketError('fake packet')
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise PacketMalformedError
        conn.mockCalledMethods = {} 
        def fake(c, p): raise PacketMalformedError('message')
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise BrokenNodeDisallowedError
        conn.mockCalledMethods = {} 
        def fake(c, p): raise BrokenNodeDisallowedError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise NotReadyError
        conn.mockCalledMethods = {} 
        def fake(c, p): raise NotReadyError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise ProtocolError
        conn.mockCalledMethods = {} 
        def fake(c, p): raise ProtocolError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)



if __name__ == '__main__':
    unittest.main()
