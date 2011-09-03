# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2010  Nexedi SA
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
from time import time
from mock import Mock
from neo.lib.connection import ListeningConnection, Connection, \
     ClientConnection, ServerConnection, MTClientConnection, \
     HandlerSwitcher, CRITICAL_TIMEOUT
from neo.lib.connector import getConnectorHandler, registerConnectorHandler
from neo.tests import DoNothingConnector
from neo.lib.connector import ConnectorException, ConnectorTryAgainException, \
     ConnectorInProgressException, ConnectorConnectionRefusedException
from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets, ParserState
from neo.tests import NeoUnitTestBase
from neo.lib.util import ReadBuffer
from neo.lib.locking import Queue

class ConnectionTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.app = Mock({'__repr__': 'Fake App'})
        self.em = Mock({'__repr__': 'Fake Em'})
        self.handler = Mock({'__repr__': 'Fake Handler'})
        self.address = ("127.0.0.7", 93413)

    def _makeListeningConnection(self, addr):
        # create instance after monkey patches
        self.connector = DoNothingConnector()
        return ListeningConnection(event_manager=self.em, handler=self.handler,
                connector=self.connector, addr=addr)

    def _makeConnection(self):
        self.connector = DoNothingConnector()
        return Connection(event_manager=self.em, handler=self.handler,
                connector=self.connector, addr=self.address)

    def _makeClientConnection(self):
        self.connector = DoNothingConnector()
        return ClientConnection(event_manager=self.em, handler=self.handler,
                connector=self.connector, addr=self.address)

    def _makeServerConnection(self):
        self.connector = DoNothingConnector()
        return ServerConnection(event_manager=self.em, handler=self.handler,
                connector=self.connector, addr=self.address)

    def _checkRegistered(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("register")), n)

    def _checkUnregistered(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("unregister")), n)

    def _checkReaderAdded(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("addReader")), n)

    def _checkReaderRemoved(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("removeReader")), n)

    def _checkWriterAdded(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("addWriter")), n)

    def _checkWriterRemoved(self, n=1):
        self.assertEqual(len(self.em.mockGetNamedCalls("removeWriter")), n)

    def _checkShutdown(self, n=1):
        self.assertEqual(len(self.connector.mockGetNamedCalls("shutdown")), n)

    def _checkClose(self, n=1):
        self.assertEqual(len(self.connector.mockGetNamedCalls("close")), n)

    def _checkGetNewConnection(self, n=1):
        calls = self.connector.mockGetNamedCalls('getNewConnection')
        self.assertEqual(len(calls), n)

    def _checkSend(self, n=1, data=None):
        calls = self.connector.mockGetNamedCalls('send')
        self.assertEqual(len(calls), n)
        if n > 1 and data is not None:
            data = calls[n-1].getParam(0)
            self.assertEqual(data, "testdata")

    def _checkConnectionAccepted(self, n=1):
        calls = self.handler.mockGetNamedCalls('connectionAccepted')
        self.assertEqual(len(calls), n)

    def _checkConnectionFailed(self, n=1):
        calls = self.handler.mockGetNamedCalls('connectionFailed')
        self.assertEqual(len(calls), n)

    def _checkConnectionClosed(self, n=1):
        calls = self.handler.mockGetNamedCalls('connectionClosed')
        self.assertEqual(len(calls), n)

    def _checkConnectionStarted(self, n=1):
        calls = self.handler.mockGetNamedCalls('connectionStarted')
        self.assertEqual(len(calls), n)

    def _checkConnectionCompleted(self, n=1):
        calls = self.handler.mockGetNamedCalls('connectionCompleted')
        self.assertEqual(len(calls), n)

    def _checkMakeListeningConnection(self, n=1):
        calls = self.connector.mockGetNamedCalls('makeListeningConnection')
        self.assertEqual(len(calls), n)

    def _checkMakeClientConnection(self, n=1):
        calls = self.connector.mockGetNamedCalls("makeClientConnection")
        self.assertEqual(len(calls), n)
        self.assertEqual(calls[n-1].getParam(0), self.address)

    def _checkPacketReceived(self, n=1):
        calls = self.handler.mockGetNamedCalls('packetReceived')
        self.assertEqual(len(calls), n)

    def _checkReadBuf(self, bc, data):
        content = bc.read_buf.read(len(bc.read_buf))
        self.assertEqual(''.join(content), data)

    def _appendToReadBuf(self, bc, data):
        bc.read_buf.append(data)

    def _appendPacketToReadBuf(self, bc, packet):
        data = ''.join(packet.encode())
        bc.read_buf.append(data)

    def _checkWriteBuf(self, bc, data):
        self.assertEqual(''.join(bc.write_buf), data)

    def test_01_BaseConnection1(self):
        # init with connector
        registerConnectorHandler(DoNothingConnector)
        connector = getConnectorHandler("DoNothingConnector")()
        self.assertFalse(connector is None)
        bc = self._makeConnection()
        self.assertFalse(bc.connector is None)
        self._checkRegistered(1)

    def test_01_BaseConnection2(self):
        # init with address
        bc = self._makeConnection()
        self.assertEqual(bc.getAddress(), self.address)
        self._checkRegistered(1)

    def test_02_ListeningConnection1(self):
        # test init part
        def getNewConnection(self):
            return self, ('', 0)
        DoNothingConnector.getNewConnection = getNewConnection
        addr = ("127.0.0.7", 93413)
        bc = self._makeListeningConnection(addr=addr)
        self.assertEqual(bc.getAddress(), addr)
        self._checkRegistered()
        self._checkReaderAdded()
        self._checkMakeListeningConnection()
        # test readable
        bc.readable()
        self._checkGetNewConnection()
        self._checkConnectionAccepted()

    def test_02_ListeningConnection2(self):
        # test with exception raise when getting new connection
        def getNewConnection(self):
            raise ConnectorTryAgainException
        DoNothingConnector.getNewConnection = getNewConnection
        addr = ("127.0.0.7", 93413)
        bc = self._makeListeningConnection(addr=addr)
        self.assertEqual(bc.getAddress(), addr)
        self._checkRegistered()
        self._checkReaderAdded()
        self._checkMakeListeningConnection()
        # test readable
        bc.readable()
        self._checkGetNewConnection(1)
        self._checkConnectionAccepted(0)

    def test_03_Connection(self):
        bc = self._makeConnection()
        self.assertEqual(bc.getAddress(), self.address)
        self._checkReaderAdded(1)
        self._checkReadBuf(bc, '')
        self._checkWriteBuf(bc, '')
        self.assertEqual(bc.cur_id, 0)
        self.assertFalse(bc.aborted)
        # test uuid
        self.assertEqual(bc.uuid, None)
        self.assertEqual(bc.getUUID(), None)
        uuid = self.getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        cur_id = bc.cur_id
        next_id = bc._getNextId()
        self.assertEqual(next_id, cur_id)
        next_id = bc._getNextId()
        self.assertTrue(next_id > cur_id)
        # test overflow of next id
        bc.cur_id =  0xffffffff
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0xffffffff)
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0)

    def test_Connection_pending(self):
        bc = self._makeConnection()
        self.assertEqual(''.join(bc.write_buf), '')
        self.assertFalse(bc.pending())
        bc.write_buf += '1'
        self.assertTrue(bc.pending())

    def test_Connection_recv1(self):
        # patch receive method to return data
        def receive(self):
            return "testdata"
        DoNothingConnector.receive = receive
        bc = self._makeConnection()
        self._checkReadBuf(bc, '')
        bc._recv()
        self._checkReadBuf(bc, 'testdata')

    def test_Connection_recv2(self):
        # patch receive method to raise try again
        def receive(self):
            raise ConnectorTryAgainException
        DoNothingConnector.receive = receive
        bc = self._makeConnection()
        self._checkReadBuf(bc, '')
        bc._recv()
        self._checkReadBuf(bc, '')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_recv3(self):
        # patch receive method to raise ConnectorConnectionRefusedException
        def receive(self):
            raise ConnectorConnectionRefusedException
        DoNothingConnector.receive = receive
        bc = self._makeConnection()
        self._checkReadBuf(bc, '')
        # fake client connection instance with connecting attribute
        bc.connecting = True
        bc._recv()
        self._checkReadBuf(bc, '')
        self._checkConnectionFailed(1)
        self._checkUnregistered(1)

    def test_Connection_recv4(self):
        # patch receive method to raise any other connector error
        def receive(self):
            raise ConnectorException
        DoNothingConnector.receive = receive
        bc = self._makeConnection()
        self._checkReadBuf(bc, '')
        self.assertRaises(ConnectorException, bc._recv)
        self._checkReadBuf(bc, '')
        self._checkConnectionClosed(1)
        self._checkUnregistered(1)

    def test_Connection_send1(self):
        # no data, nothing done
        # patch receive method to return data
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc._send()
        self._checkSend(0)
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send2(self):
        # send all data
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        bc._send()
        self._checkSend(1, "testdata")
        self._checkWriteBuf(bc, '')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send3(self):
        # send part of the data
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        bc._send()
        self._checkSend(1, "testdata")
        self._checkWriteBuf(bc, 'data')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send4(self):
        # send multiple packet
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata", "second", "third"]
        bc._send()
        self._checkSend(1, "testdatasecondthird")
        self._checkWriteBuf(bc, '')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send5(self):
        # send part of multiple packet
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata", "second", "third"]
        bc._send()
        self._checkSend(1, "testdatasecondthird")
        self._checkWriteBuf(bc, 'econdthird')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send6(self):
        # raise try again
        def send(self, data):
            raise ConnectorTryAgainException
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata", "second", "third"]
        bc._send()
        self._checkSend(1, "testdatasecondthird")
        self._checkWriteBuf(bc, 'testdatasecondthird')
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)

    def test_Connection_send7(self):
        # raise other error
        def send(self, data):
            raise ConnectorException
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata", "second", "third"]
        self.assertRaises(ConnectorException, bc._send)
        self._checkSend(1, "testdatasecondthird")
        # connection closed -> buffers flushed
        self._checkWriteBuf(bc, '')
        self._checkReaderRemoved(1)
        self._checkConnectionClosed(1)
        self._checkUnregistered(1)

    def test_07_Connection_addPacket(self):
        # new packet
        p = Mock({"encode" : "testdata", "getId": 0})
        p._body = ''
        p.handler_method_name = 'testmethod'
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc._addPacket(p)
        self._checkWriteBuf(bc, 'testdata')
        self._checkWriterAdded(1)

    def test_Connection_analyse1(self):
        # nothing to read, nothing is done
        bc = self._makeConnection()
        bc._queue = Mock()
        self._checkReadBuf(bc, '')
        bc.analyse()
        self._checkPacketReceived(0)
        self._checkReadBuf(bc, '')

        # give some data to analyse
        master_list = (
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2235), self.getNewUUID()),
                (("127.0.0.1", 2134), self.getNewUUID()),
                (("127.0.0.1", 2335), self.getNewUUID()),
                (("127.0.0.1", 2133), self.getNewUUID()),
                (("127.0.0.1", 2435), self.getNewUUID()),
                (("127.0.0.1", 2132), self.getNewUUID()))
        p = Packets.AnswerPrimary(self.getNewUUID(), master_list)
        p.setId(1)
        p_data = ''.join(p.encode())
        data_edge = len(p_data) - 1
        p_data_1, p_data_2 = p_data[:data_edge], p_data[data_edge:]
        # append an incomplete packet, nothing is done
        bc.read_buf.append(p_data_1)
        bc.analyse()
        self._checkPacketReceived(0)
        self.assertNotEqual(len(bc.read_buf), 0)
        self.assertNotEqual(len(bc.read_buf), len(p_data))
        # append the rest of the packet
        bc.read_buf.append(p_data_2)
        bc.analyse()
        # check packet decoded
        self.assertEqual(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(type(data), type(p))
        self.assertEqual(data.getId(), p.getId())
        self.assertEqual(data.decode(), p.decode())
        self._checkReadBuf(bc, '')

    def test_Connection_analyse2(self):
        # give multiple packet
        bc = self._makeConnection()
        bc._queue = Mock()
        # packet 1
        master_list = (
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2235), self.getNewUUID()),
                (("127.0.0.1", 2134), self.getNewUUID()),
                (("127.0.0.1", 2335), self.getNewUUID()),
                (("127.0.0.1", 2133), self.getNewUUID()),
                (("127.0.0.1", 2435), self.getNewUUID()),
                (("127.0.0.1", 2132), self.getNewUUID()))
        p1 = Packets.AnswerPrimary(self.getNewUUID(), master_list)
        p1.setId(1)
        self._appendPacketToReadBuf(bc, p1)
        # packet 2
        master_list = (
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2235), self.getNewUUID()),
                (("127.0.0.1", 2134), self.getNewUUID()),
                (("127.0.0.1", 2335), self.getNewUUID()),
                (("127.0.0.1", 2133), self.getNewUUID()),
                (("127.0.0.1", 2435), self.getNewUUID()),
                (("127.0.0.1", 2132), self.getNewUUID()))
        p2 = Packets.AnswerPrimary( self.getNewUUID(), master_list)
        p2.setId(2)
        self._appendPacketToReadBuf(bc, p2)
        self.assertEqual(len(bc.read_buf), len(p1) + len(p2))
        bc.analyse()
        # check two packets decoded
        self.assertEqual(len(bc._queue.mockGetNamedCalls("append")), 2)
        # packet 1
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(type(data), type(p1))
        self.assertEqual(data.getId(), p1.getId())
        self.assertEqual(data.decode(), p1.decode())
        # packet 2
        call = bc._queue.mockGetNamedCalls("append")[1]
        data = call.getParam(0)
        self.assertEqual(type(data), type(p2))
        self.assertEqual(data.getId(), p2.getId())
        self.assertEqual(data.decode(), p2.decode())
        self._checkReadBuf(bc, '')

    def test_Connection_analyse3(self):
        # give a bad packet, won't be decoded
        bc = self._makeConnection()
        bc._queue = Mock()
        self._appendToReadBuf(bc, 'datadatadatadata')
        bc.analyse()
        self.assertEqual(len(bc._queue.mockGetNamedCalls("append")), 0)
        self.assertEqual(
            len(self.handler.mockGetNamedCalls("_packetMalformed")), 1)

    def test_Connection_analyse4(self):
        # give an expected packet
        bc = self._makeConnection()
        bc._queue = Mock()
        master_list = (
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2135), self.getNewUUID()),
                (("127.0.0.1", 2235), self.getNewUUID()),
                (("127.0.0.1", 2134), self.getNewUUID()),
                (("127.0.0.1", 2335), self.getNewUUID()),
                (("127.0.0.1", 2133), self.getNewUUID()),
                (("127.0.0.1", 2435), self.getNewUUID()),
                (("127.0.0.1", 2132), self.getNewUUID()))
        p = Packets.AnswerPrimary(self.getNewUUID(), master_list)
        p.setId(1)
        self._appendPacketToReadBuf(bc, p)
        bc.analyse()
        # check packet decoded
        self.assertEqual(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(type(data), type(p))
        self.assertEqual(data.getId(), p.getId())
        self.assertEqual(data.decode(), p.decode())
        self._checkReadBuf(bc, '')

    def test_Connection_writable1(self):
        # with  pending operation after send
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        bc.writable()
        # test send was called
        self._checkSend(1, "testdata")
        self.assertEqual(''.join(bc.write_buf), "data")
        self._checkConnectionClosed(0)
        self._checkUnregistered(0)
        # pending, so nothing called
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        self._checkWriterRemoved(0)
        self._checkReaderRemoved(0)
        self._checkShutdown(0)
        self._checkClose(0)

    def test_Connection_writable2(self):
        # with no longer pending operation after send
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        bc.writable()
        # test send was called
        self._checkSend(1, "testdata")
        self._checkWriteBuf(bc, '')
        self._checkClose(0)
        self._checkUnregistered(0)
        # nothing else pending, and aborted is false, so writer has been removed
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self._checkWriterRemoved(1)
        self._checkReaderRemoved(0)
        self._checkShutdown(0)
        self._checkClose(0)

    def test_Connection_writable3(self):
        # with no longer pending operation after send and aborted set to true
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        bc = self._makeConnection()
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        self.assertTrue(bc.pending())
        bc.abort()
        self.assertTrue(bc.aborted)
        bc.writable()
        # test send was called
        self._checkSend(1, "testdata")
        self._checkWriteBuf(bc, '')
        self._checkConnectionClosed(1)
        self._checkUnregistered(1)
        # nothing else pending, and aborted is false, so writer has been removed
        self.assertFalse(bc.pending())
        self.assertTrue(bc.aborted)
        self._checkWriterRemoved(1)
        self._checkReaderRemoved(1)
        self._checkShutdown(1)
        self._checkClose(1)

    def test_Connection_readable(self):
        # With aborted set to false
        # patch receive method to return data
        def receive(self):
            master_list = ((("127.0.0.1", 2135), self.getNewUUID()),
               (("127.0.0.1", 2136), self.getNewUUID()),
               (("127.0.0.1", 2235), self.getNewUUID()),
               (("127.0.0.1", 2134), self.getNewUUID()),
               (("127.0.0.1", 2335), self.getNewUUID()),
               (("127.0.0.1", 2133), self.getNewUUID()),
               (("127.0.0.1", 2435), self.getNewUUID()),
               (("127.0.0.1", 2132), self.getNewUUID()))
            uuid = self.getNewUUID()
            p = Packets.AnswerPrimary(uuid, master_list)
            p.setId(1)
            return ''.join(p.encode())
        DoNothingConnector.receive = receive
        bc = self._makeConnection()
        bc._queue = Mock()
        self._checkReadBuf(bc, '')
        self.assertFalse(bc.aborted)
        bc.readable()
        # check packet decoded
        self._checkReadBuf(bc, '')
        self.assertEqual(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(type(data), Packets.AnswerPrimary)
        self.assertEqual(data.getId(), 1)
        self._checkReadBuf(bc, '')
        # check not aborted
        self.assertFalse(bc.aborted)
        self._checkUnregistered(0)
        self._checkWriterRemoved(0)
        self._checkReaderRemoved(0)
        self._checkShutdown(0)
        self._checkClose(0)

    def test_ClientConnection_init1(self):
        # create a good client connection
        bc = self._makeClientConnection()
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertFalse(bc.isServer())
        self._checkMakeClientConnection(1)
        # check call to handler
        self.assertFalse(bc.getHandler() is None)
        self._checkConnectionStarted(1)
        self._checkConnectionCompleted(1)
        self._checkConnectionFailed(0)
        # check call to event manager
        self.assertFalse(bc.getEventManager() is None)
        self._checkReaderAdded(1)
        self._checkWriterAdded(0)

    def test_ClientConnection_init2(self):
        # raise connection in progress
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorInProgressException
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = self._makeClientConnection()
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.isServer())
        self._checkMakeClientConnection(1)
        # check call to handler
        self.assertFalse(bc.getHandler() is None)
        self._checkConnectionStarted(1)
        self._checkConnectionCompleted(0)
        self._checkConnectionFailed(0)
        # check call to event manager
        self.assertFalse(bc.getEventManager() is None)
        self._checkReaderAdded(1)
        self._checkWriterAdded(1)

    def test_ClientConnection_init3(self):
        # raise another error, connection must fail
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorException
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            self.assertRaises(ConnectorException, self._makeClientConnection)
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # since the exception was raised, the connection is not created
        # check call to handler
        self._checkConnectionStarted(1)
        self._checkConnectionCompleted(0)
        self._checkConnectionFailed(1)
        # check call to event manager
        self._checkReaderAdded(1)
        self._checkWriterAdded(0)

    def test_ClientConnection_writable1(self):
        # with a non connecting connection, will call parent's method
        def makeClientConnection(self, *args, **kw):
            return "OK"
        def send(self, data):
            return len(data)
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        DoNothingConnector.send = send
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = self._makeClientConnection()
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        # call
        self._checkConnectionCompleted(1)
        self._checkReaderAdded(1)
        bc.writable()
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertFalse(bc.connecting)
        self._checkSend(1, "testdata")
        self._checkConnectionClosed(0)
        self._checkConnectionCompleted(1)
        self._checkConnectionFailed(0)
        self._checkUnregistered(0)
        self._checkReaderAdded(1)
        self._checkWriterRemoved(1)
        self._checkReaderRemoved(0)
        self._checkShutdown(0)
        self._checkClose(0)

    def test_ClientConnection_writable2(self):
        # with a connecting connection, must not call parent's method
        # with errors, close connection
        def getError(self):
            return True
        DoNothingConnector.getError = getError
        bc = self._makeClientConnection()
        # check connector created and connection initialize
        bc.connecting = True
        self._checkWriteBuf(bc, '')
        bc.write_buf = ["testdata"]
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        # call
        self._checkConnectionCompleted(1)
        self._checkReaderAdded(1)
        bc.writable()
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self._checkWriteBuf(bc, '')
        self._checkConnectionClosed(0)
        self._checkConnectionCompleted(1)
        self._checkConnectionFailed(1)
        self._checkUnregistered(1)
        self._checkReaderAdded(1)
        self._checkWriterRemoved(1)
        self._checkReaderRemoved(1)

    def test_14_ServerConnection(self):
        bc = self._makeServerConnection()
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self._checkReaderAdded(1)
        self._checkReadBuf(bc, '')
        self._checkWriteBuf(bc, '')
        self.assertEqual(bc.cur_id, 0)
        self.assertFalse(bc.aborted)
        # test uuid
        self.assertEqual(bc.uuid, None)
        self.assertEqual(bc.getUUID(), None)
        uuid = self.getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        cur_id = bc.cur_id
        next_id = bc._getNextId()
        self.assertEqual(next_id, cur_id)
        next_id = bc._getNextId()
        self.assertTrue(next_id > cur_id)
        # test overflow of next id
        bc.cur_id =  0xffffffff
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0xffffffff)
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0)

    def test_15_Timeout(self):
        # NOTE: This method uses ping/pong packets only because MT connection
        #       don't accept any other packet without specifying a queue.
        self.handler = EventHandler(self.app)
        conn = self._makeClientConnection()

        use_case_list = (
            # (a) For a single packet sent at T,
            #     the limit time for the answer is T + (1 * CRITICAL_TIMEOUT)
            ((), (1., 0)),
            # (b) Same as (a), even if send another packet at (T + CT/2).
            #     But receiving a packet (at T + CT - ε) resets the timeout
            #     (which means the limit for the 2nd one is T + 2*CT)
            ((.5, None), (1., 0, 2., 1)),
            # (c) Same as (b) with a first answer at well before the limit
            #     (T' = T + CT/2). The limit for the second one is T' + CT.
            ((.1, None, .5, 1), (1.5, 0)),
        )

        from neo.lib import connection
        def set_time(t):
            connection.time = lambda: int(CRITICAL_TIMEOUT * (1000 + t))
        closed = []
        conn.close = lambda: closed.append(connection.time())
        def answer(packet_id):
            p = Packets.Pong()
            p.setId(packet_id)
            conn.connector.receive = [''.join(p.encode())].pop
            conn.readable()
            conn.checkTimeout(connection.time())
            conn.process()
        try:
            for use_case, expected in use_case_list:
                i = iter(use_case)
                conn.cur_id = 0
                set_time(0)
                # No timeout when no pending request
                self.assertEqual(conn._handlers.getNextTimeout(), None)
                conn.ask(Packets.Ping())
                for t in i:
                    set_time(t)
                    conn.checkTimeout(connection.time())
                    packet_id = i.next()
                    if packet_id is None:
                        conn.ask(Packets.Ping())
                    else:
                        answer(packet_id)
                i = iter(expected)
                for t in i:
                    set_time(t - .1)
                    conn.checkTimeout(connection.time())
                    set_time(t)
                    # this test method relies on the fact that only
                    # conn.close is called in case of a timeout
                    conn.checkTimeout(connection.time())
                    self.assertEqual(closed.pop(), connection.time())
                    answer(i.next())
                self.assertFalse(conn.isPending())
                self.assertFalse(closed)
        finally:
            connection.time = time

class MTConnectionTests(ConnectionTests):
    # XXX: here we test non-client-connection-related things too, which
    # duplicates test suite work... Should be fragmented into finer-grained
    # test classes.

    def setUp(self):
        super(MTConnectionTests, self).setUp()
        self.dispatcher = Mock({'__repr__': 'Fake Dispatcher'})

    def _makeClientConnection(self):
        self.connector = DoNothingConnector()
        return MTClientConnection(event_manager=self.em, handler=self.handler,
                connector=self.connector, addr=self.address,
                dispatcher=self.dispatcher)

    def test_MTClientConnectionQueueParameter(self):
        queue = Queue()
        ask = self._makeClientConnection().ask
        packet = Packets.AskPrimary() # Any non-Ping simple "ask" packet
        # One cannot "ask" anything without a queue
        self.assertRaises(TypeError, ask, packet)
        ask(packet, queue=queue)
        # ... except Ping
        ask(Packets.Ping())

class HandlerSwitcherTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self._handler = handler = Mock({
            '__repr__': 'initial handler',
        })
        self._connection = Mock({
            '__repr__': 'connection',
            'getAddress': ('127.0.0.1', 10000),
        })
        self._handlers = HandlerSwitcher(handler)

    def _makeNotification(self, msg_id):
        packet = Packets.StartOperation()
        packet.setId(msg_id)
        return packet

    def _makeRequest(self, msg_id):
        packet = Packets.AskBeginTransaction()
        packet.setId(msg_id)
        return packet

    def _makeAnswer(self, msg_id):
        packet = Packets.AnswerBeginTransaction(self.getNextTID())
        packet.setId(msg_id)
        return packet

    def _makeHandler(self):
        return Mock({'__repr__': 'handler'})

    def _checkPacketReceived(self, handler, packet, index=0):
        calls = handler.mockGetNamedCalls('packetReceived')
        self.assertEqual(len(calls), index + 1)

    def _checkCurrentHandler(self, handler):
        self.assertTrue(self._handlers.getHandler() is handler)

    def testInit(self):
        self._checkCurrentHandler(self._handler)
        self.assertFalse(self._handlers.isPending())

    def testEmit(self):
        # First case, emit is called outside of a handler
        self.assertFalse(self._handlers.isPending())
        request = self._makeRequest(1)
        self._handlers.emit(request, 0, None)
        self.assertTrue(self._handlers.isPending())
        # Second case, emit is called from inside a handler with a pending
        # handler change.
        new_handler = self._makeHandler()
        applied = self._handlers.setHandler(new_handler)
        self.assertFalse(applied)
        self._checkCurrentHandler(self._handler)
        call_tracker = []
        def packetReceived(conn, packet):
            self._handlers.emit(self._makeRequest(2), 0, None)
            call_tracker.append(True)
        self._handler.packetReceived = packetReceived
        self._handlers.handle(self._connection, self._makeAnswer(1))
        self.assertEqual(call_tracker, [True])
        # Effective handler must not have changed (new request is blocking
        # it)
        self._checkCurrentHandler(self._handler)
        # Handling the next response will cause the handler to change
        delattr(self._handler, 'packetReceived')
        self._handlers.handle(self._connection, self._makeAnswer(2))
        self._checkCurrentHandler(new_handler)

    def testHandleNotification(self):
        # handle with current handler
        notif1 = self._makeNotification(1)
        self._handlers.handle(self._connection, notif1)
        self._checkPacketReceived(self._handler, notif1)
        # emit a request and delay an handler
        request = self._makeRequest(2)
        self._handlers.emit(request, 0, None)
        handler = self._makeHandler()
        applied = self._handlers.setHandler(handler)
        self.assertFalse(applied)
        # next notification fall into the current handler
        notif2 = self._makeNotification(3)
        self._handlers.handle(self._connection, notif2)
        self._checkPacketReceived(self._handler, notif2, index=1)
        # handle with new handler
        answer = self._makeAnswer(2)
        self._handlers.handle(self._connection, answer)
        notif3 = self._makeNotification(4)
        self._handlers.handle(self._connection, notif3)
        self._checkPacketReceived(handler, notif2)

    def testHandleAnswer1(self):
        # handle with current handler
        request = self._makeRequest(1)
        self._handlers.emit(request, 0, None)
        answer = self._makeAnswer(1)
        self._handlers.handle(self._connection, answer)
        self._checkPacketReceived(self._handler, answer)

    def testHandleAnswer2(self):
        # handle with blocking handler
        request = self._makeRequest(1)
        self._handlers.emit(request, 0, None)
        handler = self._makeHandler()
        applied = self._handlers.setHandler(handler)
        self.assertFalse(applied)
        answer = self._makeAnswer(1)
        self._handlers.handle(self._connection, answer)
        self._checkPacketReceived(self._handler, answer)
        self._checkCurrentHandler(handler)

    def testHandleAnswer3(self):
        # multiple setHandler
        r1 = self._makeRequest(1)
        r2 = self._makeRequest(2)
        r3 = self._makeRequest(3)
        a1 = self._makeAnswer(1)
        a2 = self._makeAnswer(2)
        a3 = self._makeAnswer(3)
        h1 = self._makeHandler()
        h2 = self._makeHandler()
        h3 = self._makeHandler()
        # emit all requests and setHandleres
        self._handlers.emit(r1, 0, None)
        applied = self._handlers.setHandler(h1)
        self.assertFalse(applied)
        self._handlers.emit(r2, 0, None)
        applied = self._handlers.setHandler(h2)
        self.assertFalse(applied)
        self._handlers.emit(r3, 0, None)
        applied = self._handlers.setHandler(h3)
        self.assertFalse(applied)
        self._checkCurrentHandler(self._handler)
        self.assertTrue(self._handlers.isPending())
        # process answers
        self._handlers.handle(self._connection, a1)
        self._checkCurrentHandler(h1)
        self._handlers.handle(self._connection, a2)
        self._checkCurrentHandler(h2)
        self._handlers.handle(self._connection, a3)
        self._checkCurrentHandler(h3)

    def testHandleAnswer4(self):
        # process in disorder
        r1 = self._makeRequest(1)
        r2 = self._makeRequest(2)
        r3 = self._makeRequest(3)
        a1 = self._makeAnswer(1)
        a2 = self._makeAnswer(2)
        a3 = self._makeAnswer(3)
        h = self._makeHandler()
        # emit all requests
        self._handlers.emit(r1, 0, None)
        self._handlers.emit(r2, 0, None)
        self._handlers.emit(r3, 0, None)
        applied = self._handlers.setHandler(h)
        self.assertFalse(applied)
        # process answers
        self._handlers.handle(self._connection, a1)
        self._checkCurrentHandler(self._handler)
        self._handlers.handle(self._connection, a2)
        self._checkCurrentHandler(self._handler)
        self._handlers.handle(self._connection, a3)
        self._checkCurrentHandler(h)

    def testHandleUnexpected(self):
        # process in disorder
        r1 = self._makeRequest(1)
        r2 = self._makeRequest(2)
        a2 = self._makeAnswer(2)
        h = self._makeHandler()
        # emit requests aroung state setHandler
        self._handlers.emit(r1, 0, None)
        applied = self._handlers.setHandler(h)
        self.assertFalse(applied)
        self._handlers.emit(r2, 0, None)
        # process answer for next state
        self._handlers.handle(self._connection, a2)
        self.checkAborted(self._connection)


if __name__ == '__main__':
    unittest.main()
