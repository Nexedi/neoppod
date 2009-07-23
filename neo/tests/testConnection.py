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
from mock import Mock
from neo import protocol
from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, INVALID_UUID
from neo.node import Node, MasterNode, StorageNode, ClientNode, NodeManager
from time import time
from neo.connection import BaseConnection, ListeningConnection, Connection, \
     ClientConnection, ServerConnection, MTClientConnection, MTServerConnection
from neo.connector import getConnectorHandler, registerConnectorHandler
from neo.handler import EventHandler
from neo.tests import DoNothingConnector
from neo.connector import ConnectorException, ConnectorTryAgainException, \
     ConnectorInProgressException, ConnectorConnectionRefusedException
from neo.protocol import Packet, ProtocolError, PROTOCOL_ERROR_CODE, ERROR,INTERNAL_ERROR_CODE, \
     ANSWER_PRIMARY_MASTER
from neo import protocol

def getNewUUID():
    uuid = INVALID_UUID
    while uuid == INVALID_UUID:
        uuid = os.urandom(16)
    return uuid

class ConnectionTests(unittest.TestCase):

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
        self.assertEqual(bc.handler, handler)
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
        self.failUnless(isinstance(bc.getConnector(), DoNothingConnector))
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
        
    def test_02_ListeningConnection(self):
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
        uuid = getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        cur_id = bc.cur_id
        next_id = bc._getNextId()
        self.assertEqual(next_id, cur_id)
        next_id = bc._getNextId()
        self.failUnless(next_id > cur_id)
        # test overflow of next id
        bc.cur_id =  0xffffffff
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0xffffffff)
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0)
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertRaises(NotImplementedError, bc.isServerConnection)

    def test_04_Connection_pending(self):
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


    def test_05_Connection_recv(self):
        em = Mock()
        handler = Mock()
        # patch receive method to return data
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

        # patch receive method to raise try again
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
        # patch receive method to raise ConnectorConnectionRefusedException
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
        # patch receive method to raise any other connector error
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
        self.assertEquals(len(em.mockGetNamedCalls("unregister")), 2)
        

    def test_06_Connection_send(self):
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
        
        # send all data
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

        # send part of the data
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

        # send multiple packet
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
        
        # send part of multiple packet
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

        # raise try again
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

        # raise other error
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

        # packet witch raise protocol error
        # change the max packet size and create a to big message
        # be careful not to set the max packet size < error message 
        # this part of the test is disabled because the case where a too big
        # message is send is handled in protocol.Packet.encode
#        master_list = (("127.0.0.1", 2135, getNewUUID()), ("127.0.0.1", 2135, getNewUUID()),
#                       ("127.0.0.1", 2235, getNewUUID()), ("127.0.0.1", 2134, getNewUUID()),
#                       ("127.0.0.1", 2335, getNewUUID()),("127.0.0.1", 2133, getNewUUID()),
#                       ("127.0.0.1", 2435, getNewUUID()),("127.0.0.1", 2132, getNewUUID()))
#        p = protocol.answerPrimaryMaster(getNewUUID(), master_list)
#        p.setId(1)
#        OLD_MAX_PACKET_SIZE = protocol.MAX_PACKET_SIZE
#        protocol.MAX_PACKET_SIZE = 0x55
#        
#        connector = DoNothingConnector()
#        bc = Connection(em, handler, connector_handler=DoNothingConnector,
#                        connector=connector, addr=("127.0.0.7", 93413))
#        self.assertEqual(bc.write_buf, '')
#        self.assertNotEqual(bc.getConnector(), None)
#        import pdb
#        pdb.set_trace()
#        bc._addPacket(p)
#        self.assertNotEqual(bc.write_buf, "testdata")
#        self.assertRaises(ProtocolError, p.encode)
#        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 2)
#        # check it sends error packet
#        packet = Packet.parse(bc.write_buf)
#        self.assertEqual(packet.getType(), ERROR)
#        code, message = packet.decode()
#        self.assertEqual(code, INTERNAL_ERROR_CODE)
#        self.assertEqual(message, "internal error: message too big (206)")
#        # reset value
#        protocol.MAX_PACKET_SIZE = OLD_MAX_PACKET_SIZE


    def test_08_Connection_expectMessage(self):
        # no connector -> nothing is done
        p = Mock()
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


    def test_09_Connection_analyse(self):
        # nothing to read, nothing is done
        p = Mock()
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
        master_list = ((("127.0.0.1", 2135), getNewUUID()), (("127.0.0.1", 2135), getNewUUID()),
                       (("127.0.0.1", 2235), getNewUUID()), (("127.0.0.1", 2134), getNewUUID()),
                       (("127.0.0.1", 2335), getNewUUID()),(("127.0.0.1", 2133), getNewUUID()),
                       (("127.0.0.1", 2435), getNewUUID()),(("127.0.0.1", 2132), getNewUUID()))
        p = protocol.answerPrimaryMaster(getNewUUID(), master_list)
        p.setId(1)
        data = p.encode()
        bc.read_buf += data
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

        # give multiple packet
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        bc._queue = Mock()
        # packet 1
        master_list = ((("127.0.0.1", 2135), getNewUUID()), (("127.0.0.1", 2135), getNewUUID()),
                       (("127.0.0.1", 2235), getNewUUID()), (("127.0.0.1", 2134), getNewUUID()),
                       (("127.0.0.1", 2335), getNewUUID()),(("127.0.0.1", 2133), getNewUUID()),
                       (("127.0.0.1", 2435), getNewUUID()),(("127.0.0.1", 2132), getNewUUID()))
        p1 = protocol.answerPrimaryMaster(getNewUUID(), master_list)
        p1.setId(1)
        data = p1.encode()
        bc.read_buf += data
        # packet 2
        master_list = ((("127.0.0.1", 2135), getNewUUID()), (("127.0.0.1", 2135), getNewUUID()),
                       (("127.0.0.1", 2235), getNewUUID()), (("127.0.0.1", 2134), getNewUUID()),
                       (("127.0.0.1", 2335), getNewUUID()),(("127.0.0.1", 2133), getNewUUID()),
                       (("127.0.0.1", 2435), getNewUUID()),(("127.0.0.1", 2132), getNewUUID()))
        p2 = protocol.answerPrimaryMaster( getNewUUID(), master_list)
        p2.setId(2)
        data = p2.encode()
        bc.read_buf += data
        self.assertEqual(len(bc.read_buf), len(p1.encode()) + len(p2.encode()))
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

        # give a bad packet, won't be decoded
        em = Mock()
        handler = Mock()
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

        # give an expected packet
        em = Mock()
        handler = Mock()
        bc = Connection(em, handler, connector_handler=DoNothingConnector,
                        connector=connector, addr=("127.0.0.7", 93413))
        bc._queue = Mock()
        
        master_list = ((("127.0.0.1", 2135), getNewUUID()), (("127.0.0.1", 2135), getNewUUID()),
                       (("127.0.0.1", 2235), getNewUUID()), (("127.0.0.1", 2134), getNewUUID()),
                       (("127.0.0.1", 2335), getNewUUID()),(("127.0.0.1", 2133), getNewUUID()),
                       (("127.0.0.1", 2435), getNewUUID()),(("127.0.0.1", 2132), getNewUUID()))
        p = protocol.answerPrimaryMaster(getNewUUID(), master_list)
        p.setId(1)
        data = p.encode()
        bc.read_buf += data
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

    def test_10_Connection_writable(self):
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


    def test_11_Connection_readable(self):
        # With aborted set to false
        em = Mock()
        handler = Mock()
        # patch receive method to return data
        def receive(self):
            master_list = ((("127.0.0.1", 2135), getNewUUID()), (("127.0.0.1", 2136), getNewUUID()),
                           (("127.0.0.1", 2235), getNewUUID()), (("127.0.0.1", 2134), getNewUUID()),
                           (("127.0.0.1", 2335), getNewUUID()),(("127.0.0.1", 2133), getNewUUID()),
                           (("127.0.0.1", 2435), getNewUUID()),(("127.0.0.1", 2132), getNewUUID()))
            uuid = getNewUUID()
            p = protocol.answerPrimaryMaster(uuid, master_list)
            p.setId(1)
            data = p.encode()
            return data
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
        self.assertEqual(data.getType(), ANSWER_PRIMARY_MASTER)
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


    def test_12_ClientConnection_init(self):
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        # create a good client connection
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413))
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertFalse(bc.isServerConnection())
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

        # raise connection in progress
        def makeClientConnection(self, *args, **kw):
            raise ConnectorInProgressException
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                                  addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.isServerConnection())
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

        # raise another error, connection must fail
        def makeClientConnection(self, *args, **kw):
            raise ConnectorException
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            self.assertRaises(ConnectorException, ClientConnection, em, handler, 
                    connector_handler=DoNothingConnector, addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # since the exception was raised, the connection is not created
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
        self.assertEquals(len(em.mockGetNamedCalls("addReader")), 0)
        self.assertEquals(len(em.mockGetNamedCalls("addWriter")), 0)


    def test_13_ClientConnection_writable(self):
        # with a non connecting connection, will call parent's method
        em = Mock()
        handler = Mock()
        def makeClientConnection(self, *args, **kw):
            return "OK"
        def send(self, data):
            return len(data)
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        send_org = DoNothingConnector.send
        DoNothingConnector.send = send
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = ClientConnection(em, handler, connector_handler=DoNothingConnector,
                                  addr=("127.0.0.7", 93413))
        finally:
            DoNothingConnector.send = send_org
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
        uuid = getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        cur_id = bc.cur_id
        next_id = bc._getNextId()
        self.assertEqual(next_id, cur_id)
        next_id = bc._getNextId()
        self.failUnless(next_id > cur_id)
        # test overflow of next id
        bc.cur_id =  0xffffffff
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0xffffffff)
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0)
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertTrue(bc.isServerConnection())
        

    def test_15_MTClientConnection(self):
        makeClientConnection_org = DoNothingConnector.makeClientConnection
        # same as ClientConnection, except definition of some lock
        # create a good client connection
        em = Mock()
        handler = Mock()
        dispatcher = Mock()
        connector = DoNothingConnector()
        bc = MTClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413), dispatcher=dispatcher)
        # check connector created and connection initialize
        self.assertFalse(bc.connecting)
        self.assertFalse(bc.isServerConnection())
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
        # raise connection in progress
        def makeClientConnection(self, *args, **kw):
            raise ConnectorInProgressException
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            bc = MTClientConnection(em, handler, connector_handler=DoNothingConnector,
                              addr=("127.0.0.7", 93413), dispatcher=dispatcher)
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # check connector created and connection initialize
        self.assertTrue(bc.connecting)
        self.assertFalse(bc.isServerConnection())
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

        # raise another error, connection must fail
        def makeClientConnection(self, *args, **kw):
            raise ConnectorException
        em = Mock()
        handler = Mock()
        connector = DoNothingConnector()
        DoNothingConnector.makeClientConnection = makeClientConnection
        try:
            self.assertRaises(ConnectorException, MTClientConnection, em, handler, 
                    connector_handler=DoNothingConnector, addr=("127.0.0.7", 93413), 
                    dispatcher=dispatcher)
        finally:
            DoNothingConnector.makeClientConnection = makeClientConnection_org
        # the connection is not created
        # check call to handler
        self.assertNotEqual(bc.getHandler(), None)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionStarted")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionCompleted")), 0)
        self.assertEquals(len(handler.mockGetNamedCalls("connectionFailed")), 1)
        # check call to event manager
        self.assertNotEqual(bc.getEventManager(), None)
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
        uuid = getNewUUID()
        bc.setUUID(uuid)
        self.assertEqual(bc.getUUID(), uuid)
        # test next id
        bc._lock = Mock({'_is_owned': True})
        cur_id = bc.cur_id
        next_id = bc._getNextId()
        self.assertEqual(next_id, cur_id)
        next_id = bc._getNextId()
        self.failUnless(next_id > cur_id)
        # test overflow of next id
        bc.cur_id =  0xffffffff
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0xffffffff)
        next_id = bc._getNextId()
        self.assertEqual(next_id, 0)
        # test abort
        bc.abort()
        self.assertEqual(bc.aborted, True)
        self.assertTrue(bc.isServerConnection())

        # XXX check locking ???


        
if __name__ == '__main__':
    unittest.main()
