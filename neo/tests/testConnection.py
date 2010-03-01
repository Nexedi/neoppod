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
from mock import Mock
from neo.connection import BaseConnection, ListeningConnection, Connection, \
     ClientConnection, ServerConnection, MTClientConnection, \
     MTServerConnection, HandlerSwitcher
from neo.connector import getConnectorHandler, registerConnectorHandler
from neo.handler import EventHandler
from neo.tests import DoNothingConnector
from neo.connector import ConnectorException, ConnectorTryAgainException, \
     ConnectorInProgressException, ConnectorConnectionRefusedException
from neo.protocol import Packets
from neo.tests import NeoTestBase

class ConnectionTests(NeoTestBase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_01_BaseConnection(self):
        app = Mock()
        em = Mock() #EpollEventManager()
        handler = EventHandler(app)
        # no connector
        bc = BaseConnection(em, handler)
        self.assertNotEqual(bc.em, None)
        self.assertEqual(bc.getHandler(), handler)
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEqual(bc.getHandler(), handler)
        self.assertEqual(bc.getUUID(), None)
        self.assertEqual(bc.lock(), 1)
        self.assertEqual(bc.unlock(), None)
        self.assertEqual(bc.getAddress(), None)
        self.assertEqual(bc.getConnector(), None)
        self.assertRaises(NotImplementedError, bc.readable)
        self.assertRaises(NotImplementedError, bc.writable)
        self.assertEqual(bc.connector, None)
        self.assertEqual(bc.addr, None)
        self.assertEqual(bc.connector_handler, None)

        # init with connector but no handler
        registerConnectorHandler(DoNothingConnector)
        connector = getConnectorHandler("DoNothingConnector")()
        self.assertNotEqual(connector, None)
        em = Mock()
        bc = BaseConnection(em, handler, connector=connector)
        self.assertNotEqual(bc.connector, None)
        self.assertNotEqual(bc.getConnector(), None)
        self.assertEqual(bc.connector_handler, DoNothingConnector)
        # check it registered the connection in epoll
        self.assertEquals(len(em.mockGetNamedCalls("register")), 1)
        call = em.mockGetNamedCalls("register")[0]
        conn = call.getParam(0)
        self.assertEquals(conn, bc)

        # init just with handler
        em = Mock()
        bc = BaseConnection(em, handler, connector_handler=DoNothingConnector)
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(bc.connector_handler, DoNothingConnector)
        self.assertEquals(len(em.mockGetNamedCalls("register")), 0)

        # add connector
        connector = bc.connector_handler()
        bc.setConnector(connector)
        self.assertTrue(isinstance(bc.getConnector(), DoNothingConnector))
        self.assertNotEqual(bc.getConnector(), None)
        # check it registered the connection in epoll
        self.assertEquals(len(em.mockGetNamedCalls("register")), 1)
        call = em.mockGetNamedCalls("register")[0]
        conn = call.getParam(0)
        self.assertEquals(conn, bc)

        # init with address
        connector = DoNothingConnector()
        em = Mock()
        handler = EventHandler(app)
        bc = BaseConnection(em, handler, connector_handler=DoNothingConnector,
                            connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        # check it registered the connection in epoll
        self.assertEquals(len(em.mockGetNamedCalls("register")), 1)
        call = em.mockGetNamedCalls("register")[0]
        conn = call.getParam(0)
        self.assertEquals(conn, bc)

    def test_02_ListeningConnection1(self):
        # test init part
        em = Mock()
        handler = Mock()
        def getNewConnection(self):
            return self, "127.0.0.1"
        DoNothingConnector.getNewConnection = getNewConnection
        bc = ListeningConnection(em, handler, connector_handler=DoNothingConnector,
                            connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self.assertEqual(len(em.mockGetNamedCalls("register")), 1)
        self.assertEqual(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertNotEqual(bc.getConnector(), None)
        connector = bc.getConnector()
        self.assertEqual(len(connector.mockGetNamedCalls("makeListeningConnection")), 1)
        # test readable
        bc.readable()
        self.assertEqual(len(connector.mockGetNamedCalls("getNewConnection")), 1)
        self.assertEqual(len(handler.mockGetNamedCalls("connectionAccepted")), 1)

    def test_02_ListeningConnection2(self):
        # test with exception raise when getting new connection
        em = Mock()
        handler = Mock()
        def getNewConnection(self):
            raise ConnectorTryAgainException
        DoNothingConnector.getNewConnection = getNewConnection
        bc = ListeningConnection(em, handler, connector_handler=DoNothingConnector,
                            connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self.assertEqual(len(em.mockGetNamedCalls("register")), 1)
        self.assertEqual(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertNotEqual(bc.getConnector(), None)
        connector = bc.getConnector()
        self.assertEqual(len(connector.mockGetNamedCalls("makeListeningConnection")), 1)
        # test readable
        bc.readable()
        self.assertEqual(len(connector.mockGetNamedCalls("getNewConnection")), 1)
        self.assertEqual(len(handler.mockGetNamedCalls("connectionAccepted")), 0)

    def test_03_Connection(self):
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self.assertEqual(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(bc.read_buf, '')
        self.assertEqual(bc.write_buf, '')
        self.assertEqual(bc.cur_id, 0)
        self.assertEqual(bc.event_dict, {})
        self.assertEqual(bc.aborted, False)
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
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertFalse(bc.isServer())

    def test_Connection_pending(self):
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        self.assertEqual(bc.connector, None)
        # no connector and no buffer
        self.assertFalse(bc.pending())
        # no connector but buffer
        bc.write_buf += '1'
        self.assertFalse(bc.pending())
        # connector with no buffer
        conn = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=conn, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        self.assertNotEqual(bc.connector, None)
        self.assertFalse(bc.pending())
        # connector and buffer
        bc.write_buf += '1'
        self.assertTrue(bc.pending())


    def test_Connection_recv1(self):
        # patch receive method to return data
        em = Mock()
        handler = Mock()
        def receive(self):
            return "testdata"
        DoNothingConnector.receive = receive
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.read_buf, '')
        self.assertNotEqual(bc.getConnector(), None)

        bc._recv()
        self.assertEqual(bc.read_buf, "testdata")

    def test_Connection_recv2(self):
        # patch receive method to raise try again
        em = Mock()
        handler = Mock()
        def receive(self):
            raise ConnectorTryAgainException
        DoNothingConnector.receive = receive
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.read_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        bc._recv()
        self.assertEqual(bc.read_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_recv3(self):
        # patch receive method to raise ConnectorConnectionRefusedException
        em = Mock()
        handler = Mock()
        def receive(self):
            raise ConnectorConnectionRefusedException
        DoNothingConnector.receive = receive
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.read_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        # fake client connection instance with connecting attribute
        bc.connecting = True
        bc._recv()
        self.assertEqual(bc.read_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 1)

    def test_Connection_recv4(self):
        # patch receive method to raise any other connector error
        em = Mock()
        handler = Mock()
        def receive(self):
            raise ConnectorException
        DoNothingConnector.receive = receive
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.read_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        self.assertRaises(ConnectorException, bc._recv)
        self.assertEqual(bc.read_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 1)


    def test_Connection_send1(self):
        # no data, nothing done
        em = Mock()
        handler = Mock()
        # patch receive method to return data
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send2(self):
        # send all data
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send3(self):
        # send part of the data
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, "data")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send4(self):
        # send multiple packet
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata" + "second" + "third"
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdatasecondthird")
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send5(self):
        # send part of multiple packet
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata" + "second" + "third"
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdatasecondthird")
        self.assertEqual(bc.write_buf, "econdthird")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send6(self):
        # raise try again
        em = Mock()
        handler = Mock()
        def send(self, data):
            raise ConnectorTryAgainException
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata" + "second" + "third"
        self.assertNotEqual(bc.getConnector(), None)
        bc._send()
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdatasecondthird")
        self.assertEqual(bc.write_buf, "testdata" + "second" + "third")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)

    def test_Connection_send7(self):
        # raise other error
        em = Mock()
        handler = Mock()
        def send(self, data):
            raise ConnectorException
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata" + "second" + "third"
        self.assertNotEqual(bc.getConnector(), None)
        self.assertRaises(ConnectorException, bc._send)
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdatasecondthird")
        # connection closed -> buffers flushed
        self.assertEqual(bc.write_buf, "")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 1)

    def test_07_Connection_addPacket(self):
        # no connector
        p = Mock({"encode" : "testdata"})
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(bc.write_buf, '')
        bc._addPacket(p)
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)

        # new packet
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        bc._addPacket(p)
        self.assertEqual(bc.write_buf, "testdata")
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 1)


    def test_08_Connection_expectMessage(self):
        # no connector -> nothing is done
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=None, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(len(bc.event_dict), 0)
        bc.expectMessage('1')
        self.assertEqual(len(bc.event_dict), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addIdleEvent")), 0)

        # with a right connector -> event created
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertNotEqual(bc.getConnector(), None)
        self.assertEqual(len(bc.event_dict), 0)
        bc.expectMessage('1')
        self.assertEqual(len(bc.event_dict), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addIdleEvent")), 1)


    def test_Connection_analyse1(self):
        # nothing to read, nothing is done
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        bc._queue = Mock()
        self.assertEqual(bc.read_buf, '')
        self.assertEqual(len(bc.event_dict), 0)
        bc.analyse()
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("packetReceived")), 0)
        self.assertEqual(bc.read_buf, '')
        self.assertEqual(len(bc.event_dict), 0)

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
        bc.read_buf += p.encode()
        self.assertEqual(len(bc.event_dict), 0)
        bc.analyse()
        # check packet decoded
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 0)
        self.assertEquals(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(data.getType(), p.getType())
        self.assertEqual(data.getId(), p.getId())
        self.assertEqual(data.decode(), p.decode())
        self.assertEqual(len(bc.event_dict), 0)
        self.assertEqual(bc.read_buf, '')

    def test_Connection_analyse2(self):
        # give multiple packet
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
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
        bc.read_buf += p1.encode()
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
        bc.read_buf += p2.encode()
        self.assertEqual(len(bc.read_buf), len(p1) + len(p2))
        self.assertEqual(len(bc.event_dict), 0)
        bc.analyse()
        # check two packets decoded
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 0)
        self.assertEquals(len(bc._queue.mockGetNamedCalls("append")), 2)
        # packet 1
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(data.getType(), p1.getType())
        self.assertEqual(data.getId(), p1.getId())
        self.assertEqual(data.decode(), p1.decode())
        # packet 2
        call = bc._queue.mockGetNamedCalls("append")[1]
        data = call.getParam(0)
        self.assertEqual(data.getType(), p2.getType())
        self.assertEqual(data.getId(), p2.getId())
        self.assertEqual(data.decode(), p2.decode())
        self.assertEqual(len(bc.event_dict), 0)
        self.assertEqual(len(bc.read_buf), 0)

    def test_Connection_analyse3(self):
        # give a bad packet, won't be decoded
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        bc._queue = Mock()
        bc.read_buf += "datadatadatadata"
        self.assertEqual(len(bc.read_buf), 16)
        self.assertEqual(len(bc.event_dict), 0)
        bc.analyse()
        self.assertEqual(len(bc.read_buf), 16)
        self.assertEquals(len(bc._queue.mockGetNamedCalls("append")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 0)

    def test_Connection_analyse4(self):
        # give an expected packet
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
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
        bc.read_buf += p.encode()
        self.assertEqual(len(bc.event_dict), 0)
        bc.expectMessage(1)
        self.assertEqual(len(bc.event_dict), 1)
        bc.analyse()
        # check packet decoded
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 1)
        self.assertEquals(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(data.getType(), p.getType())
        self.assertEqual(data.getId(), p.getId())
        self.assertEqual(data.decode(), p.decode())
        self.assertEqual(len(bc.event_dict), 0)
        self.assertEqual(bc.read_buf, '')

    def test_Connection_writable1(self):
        # with  pending operation after send
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)/2
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertNotEqual(bc.getConnector(), None)
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        bc.writable()
        # test send was called
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, "data")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)
        # pending, so nothing called
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("shutdown")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("close")), 0)

    def test_Connection_writable2(self):
        # with no longer pending operation after send
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertNotEqual(bc.getConnector(), None)
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        bc.writable()
        # test send was called
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)
        # nothing else pending, and aborted is false, so writer has been removed
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("shutdown")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("close")), 0)

    def test_Connection_writable3(self):
        # with no longer pending operation after send and aborted set to true
        em = Mock()
        handler = Mock()
        def send(self, data):
            return len(data)
        DoNothingConnector.send = send
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertNotEqual(bc.getConnector(), None)
        self.assertTrue(bc.pending())
        bc.abort()
        self.assertTrue(bc.aborted)
        bc.writable()
        # test send was called
        self.assertEquals(len(connector.mockGetNamedCalls("send")), 1)
        call = connector.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 1)
        # nothing else pending, and aborted is false, so writer has been removed
        self.assertFalse(bc.pending())
        self.assertTrue(bc.aborted)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 1)
        self.assertEquals(len(connector.mockGetNamedCalls("shutdown")), 1)
        self.assertEquals(len(connector.mockGetNamedCalls("close")), 1)

    def test_Connection_readable(self):
        # With aborted set to false
        em = Mock()
        handler = Mock()
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
            return p.encode()
        DoNothingConnector.receive = receive
        connector = DoNothingConnector()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        bc._queue = Mock()
        self.assertEqual(bc.read_buf, '')
        self.assertNotEqual(bc.getConnector(), None)
        self.assertFalse(bc.aborted)
        bc.readable()
        # check packet decoded
        self.assertEqual(bc.read_buf, '')
        self.assertEquals(len(em.mockGetNamedCalls("removeIdleEvent")), 0)
        self.assertEquals(len(bc._queue.mockGetNamedCalls("append")), 1)
        call = bc._queue.mockGetNamedCalls("append")[0]
        data = call.getParam(0)
        self.assertEqual(data.getType(), Packets.AnswerPrimary)
        self.assertEqual(data.getId(), 1)
        self.assertEqual(len(bc.event_dict), 0)
        self.assertEqual(bc.read_buf, '')
        # check not aborted
        self.assertFalse(bc.aborted)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("shutdown")), 0)
        self.assertEquals(len(connector.mockGetNamedCalls("close")), 0)

    def test_ClientConnection_init1(self):
        # create a good client connection
        em = Mock()
        handler = Mock()
        bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertFalse(bc.isServer())
        self.assertNotEqual(bc.getConnector(), None)
        conn = bc.getConnector()
        self.assertEquals(len(conn.mockGetNamedCalls("makeClientConnection")), 1)
        call = conn.mockGetNamedCalls("makeClientConnection")[0]
        data = call.getParam(0)
        self.assertEqual(data, ("127.0.0.7", 93413))
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)

    def test_ClientConnection_init2(self):
        # raise connection in progress
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorInProgressException
        em = Mock()
        handler = Mock()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                                  addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.isServer())
        self.assertNotEqual(bc.getConnector(), None)
        conn = bc.getConnector()
        self.assertEquals(len(conn.mockGetNamedCalls("makeClientConnection")), 1)
        call = conn.mockGetNamedCalls("makeClientConnection")[0]
        data = call.getParam(0)
        self.assertEqual(data, ("127.0.0.7", 93413))
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 1)

    def test_ClientConnection_init3(self):
        # raise another error, connection must fail
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorException
        em = Mock()
        handler = Mock()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            self.assertRaises(ConnectorException, ClientConnection, em, handler,
                    connector_handler=DoNothingConnector, addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # since the exception was raised, the connection is not created
        # check call to handler
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        # check call to event manager
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)

    def test_ClientConnection_writable1(self):
        # with a non connecting connection, will call parent's method
        em = Mock()
        handler = Mock()
        def makeClientConnection(self, *args, **kw):
            return "OK"
        def send(self, data):
            return len(data)
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        DoNothingConnector.send = send
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                                  addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertNotEqual(bc.getConnector(), None)
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        # call
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        bc.writable()
        conn = bc.getConnector()
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertFalse(bc.connecting)
        self.assertEquals(len(conn.mockGetNamedCalls("send")), 1)
        call = conn.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")
        self.assertEqual(bc.write_buf, '')
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("shutdown")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("send")), 1)
        call = conn.mockGetNamedCalls("send")[0]
        data = call.getParam(0)
        self.assertEquals(data, "testdata")

    def test_ClientConnection_writable2(self):
        # with a connecting connection, must not call parent's method
        # with no error, just complete connection
        em = Mock()
        handler = Mock()
        def getError(self):
            return None
        DoNothingConnector.getError = getError
        bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        # check connector created and connection initialize
        bc.connecting = True
        self.assertNotEqual(bc.getConnector(), None)
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        # call
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        bc.writable()
        conn = bc.getConnector()
        self.assertFalse(bc.connecting)
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertEqual(bc.write_buf, "testdata")
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 2)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 2)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("send")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("shutdown")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)

    def test_ClientConnection_writable3(self):
        # with a connecting connection, must not call parent's method
        # with errors, close connection
        em = Mock()
        handler = Mock()
        def getError(self):
            return True
        DoNothingConnector.getError = getError
        bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        # check connector created and connection initialize
        bc.connecting = True
        self.assertNotEqual(bc.getConnector(), None)
        self.assertEqual(bc.write_buf, '')
        bc.write_buf = "testdata"
        self.assertTrue(bc.pending())
        self.assertFalse(bc.aborted)
        # call
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        bc.writable()
        self.assertEqual(bc.getConnector(), None)
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.pending())
        self.assertFalse(bc.aborted)
        self.assertEqual(bc.write_buf, "") # buffer flushed at closure
        self.assertEquals(len(handler.mockGetNamedCalls("connectionClosed")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeWriter")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("removeReader")), 1)

    def test_14_ServerConnection(self):
        em = Mock()
        handler = Mock()
        bc = ServerConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self.assertEqual(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(bc.read_buf, '')
        self.assertEqual(bc.write_buf, '')
        self.assertEqual(bc.cur_id, 0)
        self.assertEqual(bc.event_dict, {})
        self.assertEqual(bc.aborted, False)
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
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertTrue(bc.isServer())


    def test_MTClientConnection1(self):
        # same as ClientConnection, except definition of some lock
        # create a good client connection
        em = Mock()
        handler = Mock()
        dispatcher = Mock()
        bc = MTClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413), dispatcher=dispatcher)
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertFalse(bc.isServer())
        self.assertNotEqual(bc.getConnector(), None)
        conn = bc.getConnector()
        self.assertEquals(len(conn.mockGetNamedCalls("makeClientConnection")), 1)
        call = conn.mockGetNamedCalls("makeClientConnection")[0]
        data = call.getParam(0)
        self.assertEqual(data, ("127.0.0.7", 93413))
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 1)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)

    def test_MTClientConnection2(self):
        # raise connection in progress
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorInProgressException
        em = Mock()
        handler = Mock()
        dispatcher = Mock()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = MTClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413), dispatcher=dispatcher)
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.isServer())
        self.assertNotEqual(bc.getConnector(), None)
        conn = bc.getConnector()
        self.assertEquals(len(conn.mockGetNamedCalls("makeClientConnection")), 1)
        call = conn.mockGetNamedCalls("makeClientConnection")[0]
        data = call.getParam(0)
        self.assertEqual(data, ("127.0.0.7", 93413))
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 0)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 1)

    def test_MTClientConnection3(self):
        # raise another error, connection must fail
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        def makeClientConnection(self, *args, **kw):
            raise ConnectorException
        em = Mock()
        handler = Mock()
        dispatcher = Mock()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            self.assertRaises(ConnectorException, MTClientConnection, em, handler,
                    connector_handler=DoNothingConnector, addr=("127.0.0.7", 93413),
                    dispatcher=dispatcher)
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # the connection is not created
        # check call to handler
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        # check call to event manager
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)

        # XXX check locking ?


    def test_16_MTServerConnection(self):
        em = Mock()
        handler = Mock()
        bc = MTServerConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        self.assertEqual(bc.getAddress(), ("127.0.0.7", 93413))
        self.assertEqual(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEqual(bc.getConnector(), None)
        self.assertEqual(bc.read_buf, '')
        self.assertEqual(bc.write_buf, '')
        self.assertEqual(bc.cur_id, 0)
        self.assertEqual(bc.event_dict, {})
        self.assertEqual(bc.aborted, False)
        # test uuid
        self.assertEqual(bc.uuid, None)
        self.assertEqual(bc.getUUID(), None)
        uuid = self.getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        bc._lock = Mock({'_is_owned': True})
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
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertTrue(bc.isServer())

        # XXX check locking ???

class HandlerSwitcherTests(NeoTestBase):

    def setUp(self):
        self._handler = handler = Mock({
            '__repr__': 'initial handler',
        })
        self._connection = connection = Mock({
            '__repr__': 'connection',
            'getAddress': ('127.0.0.1', 10000),
        })
        self._handlers = HandlerSwitcher(connection, handler)

    def _makeNotification(self, msg_id):
        packet = Packets.StartOperation()
        packet.setId(msg_id)
        return packet

    def _makeRequest(self, msg_id):
        packet = Packets.AskBeginTransaction(self.getNextTID())
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
        self.assertFalse(self._handlers.isPending())
        request = self._makeRequest(1)
        self._handlers.emit(request)
        self.assertTrue(self._handlers.isPending())

    def testHandleNotification(self):
        # handle with current handler
        notif1 = self._makeNotification(1)
        self._handlers.handle(notif1)
        self._checkPacketReceived(self._handler, notif1)
        # emit a request and delay an handler
        request = self._makeRequest(2)
        self._handlers.emit(request)
        handler = self._makeHandler()
        self._handlers.setHandler(handler)
        # next notification fall into the current handler
        notif2 = self._makeNotification(3)
        self._handlers.handle(notif2)
        self._checkPacketReceived(self._handler, notif2, index=1)
        # handle with new handler
        answer = self._makeAnswer(2)
        self._handlers.handle(answer)
        notif3 = self._makeNotification(4)
        self._handlers.handle(notif3)
        self._checkPacketReceived(handler, notif2)

    def testHandleAnswer1(self):
        # handle with current handler
        request = self._makeRequest(1)
        self._handlers.emit(request)
        answer = self._makeAnswer(1)
        self._handlers.handle(answer)
        self._checkPacketReceived(self._handler, answer)

    def testHandleAnswer2(self):
        # handle with blocking handler
        request = self._makeRequest(1)
        self._handlers.emit(request)
        handler = self._makeHandler()
        self._handlers.setHandler(handler)
        answer = self._makeAnswer(1)
        self._handlers.handle(answer)
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
        self._handlers.emit(r1)
        self._handlers.setHandler(h1)
        self._handlers.emit(r2)
        self._handlers.setHandler(h2)
        self._handlers.emit(r3)
        self._handlers.setHandler(h3)
        self._checkCurrentHandler(self._handler)
        self.assertTrue(self._handlers.isPending())
        # process answers
        self._handlers.handle(a1)
        self._checkCurrentHandler(h1)
        self._handlers.handle(a2)
        self._checkCurrentHandler(h2)
        self._handlers.handle(a3)
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
        self._handlers.emit(r1)
        self._handlers.emit(r2)
        self._handlers.emit(r3)
        self._handlers.setHandler(h)
        # process answers
        self._handlers.handle(a1)
        self._checkCurrentHandler(self._handler)
        self._handlers.handle(a2)
        self._checkCurrentHandler(self._handler)
        self._handlers.handle(a3)
        self._checkCurrentHandler(h)

    def testHandleUnexpected(self):
        # process in disorder
        r1 = self._makeRequest(1)
        r2 = self._makeRequest(2)
        a2 = self._makeAnswer(2)
        h = self._makeHandler()
        # emit requests aroung state setHandler
        self._handlers.emit(r1)
        self._handlers.setHandler(h)
        self._handlers.emit(r2)
        # process answer for next state
        self._handlers.handle(a2)
        self.checkAborted(self._connection)


if __name__ == '__main__':
    unittest.main()
