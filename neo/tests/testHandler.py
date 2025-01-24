#
# Copyright (C) 2009-2019  Nexedi SA
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

from . import MockObject, NeoUnitTestBase
from neo.lib.exception import PacketMalformedError, UnexpectedPacketError, \
    NotReadyError, ProtocolError
from neo.lib.handler import EventHandler


class HandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.handler = EventHandler(self.getFakeApplication())

    def setFakeMethod(self, method):
        self.handler.fake_method = method

    def getFakePacket(self):
        p = MockObject('Fake Packet', getArgs=())
        p.handler_method_name = 'fake_method'
        return p

    def test_dispatch(self):
        conn = self.getFakeConnection()
        packet = self.getFakePacket()
        # all is ok
        self.setFakeMethod(lambda c: None)
        self.handler.dispatch(conn, packet)
        # raise UnexpectedPacketError
        def fake(c):
            raise UnexpectedPacketError('fake packet')
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise NotReadyError
        def fake(c):
            raise NotReadyError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
        # raise ProtocolError
        def fake(c):
            raise ProtocolError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        self.checkAborted(conn)
