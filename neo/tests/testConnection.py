# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2016  Nexedi SA
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
    MTClientConnection, CRITICAL_TIMEOUT
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

if __name__ == '__main__':
    unittest.main()
