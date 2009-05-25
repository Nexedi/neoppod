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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest, os
from mock import Mock, ReturnValues
from neo.tests.base import NeoTestBase
from neo.handler import EventHandler
from neo.handler import identification_required, restrict_node_types, \
        client_connection_required, server_connection_required
from neo.protocol import UnexpectedPacketError, MASTER_NODE_TYPE, \
        CLIENT_NODE_TYPE, STORAGE_NODE_TYPE, ADMIN_NODE_TYPE
from neo.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError

class HandlerDecoratorsTest(NeoTestBase):

    class FakeApp(object):
        nm = None

    def setUp(self):
        self.handler_called = False
        self.app = HandlerDecoratorsTest.FakeApp()

    def fakeHandler(self, conn, packet):
        self.handler_called = True

    def checkHandlerCalled(self, handler):
        calls = handler.mockGetNamedCalls('__call__')
        self.assertEquals(len(calls), 1)

    def checkHandlerNotCalled(self, handler):
        calls = handler.mockGetNamedCalls('__call__')
        self.assertEquals(len(calls), 0)

    def test_identification_required(self):
        packet = Mock({})
        handler = Mock({})
        wrapped = identification_required(handler)
        # no UUID -> fail
        conn = Mock({ 'getUUID': None, })
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet)
        self.checkHandlerNotCalled(handler)
        # UUID set -> ok
        conn = Mock({ 'getUUID': '\x01' * 8, })
        wrapped(self, conn, packet)
        self.checkHandlerCalled(handler)

    def test_restrict_node_types(self):
        uuid = self.getNewUUID()
        packet = Mock({})
        handler = Mock({})
        storage = Mock({'getNodeType': STORAGE_NODE_TYPE})
        master = Mock({'getNodeType': MASTER_NODE_TYPE})
        client = Mock({'getNodeType': CLIENT_NODE_TYPE})
        admin = Mock({'getNodeType': ADMIN_NODE_TYPE})
        nodes = (storage, master, client, admin)
        # no uuid -> fail
        wrapped = restrict_node_types()(handler)
        conn = Mock({ 'getUUID': None, })
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet)
        self.checkHandlerNotCalled(handler)
        # unknown node -> fail
        wrapped = restrict_node_types()(handler)
        self.app.nm = Mock({'getNodeByUUID': uuid, })
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet)
        self.checkHandlerNotCalled(handler)
        # no node allowed at all -> all fail
        wrapped = restrict_node_types()(handler)
        conn = Mock({ 'getUUID': uuid, })
        self.app.nm = Mock({'getNodeByUUID': ReturnValues(*nodes)})
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        # Only master nodes allowed
        handler = Mock() 
        wrapped = restrict_node_types(MASTER_NODE_TYPE)(handler)
        conn = Mock({ 'getUUID': uuid, })
        self.app.nm = Mock({'getNodeByUUID': ReturnValues(*nodes)})
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.checkHandlerNotCalled(handler)
        wrapped(self, conn, packet) # admin
        self.checkHandlerCalled(handler)
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.checkHandlerCalled(handler) # fail if re-called
        # storage or client nodes
        handler = Mock() 
        wrapped = restrict_node_types(STORAGE_NODE_TYPE, CLIENT_NODE_TYPE)(handler)
        conn = Mock({ 'getUUID': uuid, })
        self.app.nm = Mock({'getNodeByUUID': ReturnValues(*nodes)})
        wrapped(self, conn, packet) # storage
        self.checkHandlerCalled(handler)
        handler.mockCalledMethods = {} # reset mock object
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.checkHandlerNotCalled(handler)
        wrapped(self, conn, packet) # client
        self.checkHandlerCalled(handler)
        handler.mockCalledMethods = {} # reset mock object
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet) 
        self.checkHandlerNotCalled(handler) 

    def test_client_connection_required(self):
        packet = Mock({})
        handler = Mock({})
        wrapped = client_connection_required(handler)
        # server connection -> fail
        conn = Mock({ 'isServerConnection': True, })
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet)
        self.checkHandlerNotCalled(handler)
        # client connection -> ok
        conn = Mock({ 'isServerConnection': False, })
        wrapped(self, conn, packet)
        self.checkHandlerCalled(handler)

    def test_server_connection_required(self):
        packet = Mock({})
        handler = Mock({})
        wrapped = server_connection_required(handler)
        # client connection -> fail
        conn = Mock({ 'isServerConnection': False, })
        self.assertRaises(UnexpectedPacketError, wrapped, self, conn, packet)
        self.checkHandlerNotCalled(handler)
        # server connection -> ok
        conn = Mock({ 'isServerConnection': True, })
        wrapped(self, conn, packet)
        self.checkHandlerCalled(handler)


class HandlerTest(NeoTestBase):

    def setUp(self):
        self.handler = EventHandler()
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
        # raise KeyError and ValueError
        conn.mockCalledMethods = {} 
        def fake(c, p): raise KeyError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
        conn.mockCalledMethods = {} 
        def fake(c, p): raise ValueError
        self.setFakeMethod(fake)
        self.handler.dispatch(conn, packet)
        self.checkErrorPacket(conn)
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
