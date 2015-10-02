# -*- coding: utf-8 -*-
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
from time import time
from mock import Mock
from neo.lib import connection, logging
from neo.lib.connection import BaseConnection, ClientConnection, \
    MTClientConnection, HandlerSwitcher, CRITICAL_TIMEOUT
from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets
from . import NeoUnitTestBase, Patch


connector_cpt = 0

class DummyConnector(Mock):
    def __init__(self, addr, s=None):
        logging.info("initializing connector")
        global connector_cpt
        self.desc = connector_cpt
        connector_cpt += 1
        self.packet_cpt = 0
        self.addr = addr
        Mock.__init__(self)

    def getAddress(self):
        return self.addr

    def getDescriptor(self):
        return self.desc

    accept = getError = makeClientConnection = makeListeningConnection = \
    receive = send = lambda *args, **kw: None


dummy_connector = Patch(BaseConnection,
    ConnectorClass=lambda orig, self, *args, **kw: DummyConnector(*args, **kw))


class ConnectionTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.app = Mock({'__repr__': 'Fake App'})
        self.app.ssl = None
        self.em = self.app.em = Mock({'__repr__': 'Fake Em'})
        self.handler = Mock({'__repr__': 'Fake Handler'})
        self.address = ("127.0.0.7", 93413)
        self.node = Mock({'getAddress': self.address})

    def _makeClientConnection(self):
        with dummy_connector:
            conn = ClientConnection(self.app, self.handler, self.node)
        self.connector = conn.connector
        return conn

    def testTimeout(self):
        # NOTE: This method uses ping/pong packets only because MT connections
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

        def set_time(t):
            connection.time = lambda: int(CRITICAL_TIMEOUT * (1000 + t))
        closed = []
        conn.close = lambda: closed.append(connection.time())
        def answer(packet_id):
            p = Packets.Pong()
            p.setId(packet_id)
            conn.connector.receive = lambda read_buf: \
                read_buf.append(''.join(p.encode()))
            conn.readable()
            checkTimeout()
            conn.process()
        def checkTimeout():
            timeout = conn.getTimeout()
            if timeout and timeout <= connection.time():
                conn.onTimeout()
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
                    checkTimeout()
                    packet_id = i.next()
                    if packet_id is None:
                        conn.ask(Packets.Ping())
                    else:
                        answer(packet_id)
                i = iter(expected)
                for t in i:
                    set_time(t - .1)
                    checkTimeout()
                    set_time(t)
                    # this test method relies on the fact that only
                    # conn.close is called in case of a timeout
                    checkTimeout()
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
        with dummy_connector:
            conn = MTClientConnection(self.app, self.handler, self.node,
                                      dispatcher=self.dispatcher)
        self.connector = conn.connector
        return conn

    def test_MTClientConnectionQueueParameter(self):
        ask = self._makeClientConnection().ask
        packet = Packets.AskPrimary() # Any non-Ping simple "ask" packet
        # One cannot "ask" anything without a queue
        self.assertRaises(TypeError, ask, packet)
        ask(packet, queue=object())
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
        def packetReceived(conn, packet, kw):
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
        # process out of order
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
        # process out of order
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
