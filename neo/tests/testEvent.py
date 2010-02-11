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
from time import time
from neo.tests import NeoTestBase
from neo.epoll import Epoll
from neo.event import EpollEventManager, IdleEvent

class EventTests(NeoTestBase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01_EpollEventManager(self):
        # init one
        em = EpollEventManager()
        self.assertEqual(len(em.connection_dict), 0)
        self.assertEqual(len(em.reader_set), 0)
        self.assertEqual(len(em.writer_set), 0)
        self.assertEqual(len(em.event_list), 0)
        self.assertTrue(em.prev_time <time)
        self.assertTrue(isinstance(em.epoll, Epoll))
        # use a mock object instead of epoll
        em.epoll = Mock()
        connector = Mock({"getDescriptor" : 1014})
        conn = Mock({'getConnector': connector})
        self.assertEqual(len(em.getConnectionList()), 0)

        # test register/unregister
        em.register(conn)
        self.assertEquals(len(connector.mockGetNamedCalls("getDescriptor")), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("register")), 1)
        call = em.epoll.mockGetNamedCalls("register")[0]
        data = call.getParam(0)
        self.assertEqual(data, 1014)
        self.assertEqual(len(em.getConnectionList()), 1)
        self.assertEqual(em.getConnectionList()[0].getDescriptor(), conn.getDescriptor())
        connector = Mock({"getDescriptor" : 1014})
        conn = Mock({'getConnector': connector})
        em.unregister(conn)
        self.assertEquals(len(connector.mockGetNamedCalls("getDescriptor")), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("unregister")), 1)
        call = em.epoll.mockGetNamedCalls("unregister")[0]
        data = call.getParam(0)
        self.assertEqual(data, 1014)
        self.assertEqual(len(em.getConnectionList()), 0)

        # add/removeIdleEvent
        event = Mock()
        self.assertEqual(len(em.event_list), 0)
        em.addIdleEvent(event)
        self.assertEqual(len(em.event_list), 1)
        em.removeIdleEvent(event)
        self.assertEqual(len(em.event_list), 0)
        em.removeIdleEvent(event) # must not fail
        self.assertEqual(len(em.event_list), 0)

        # add/removeReader
        connector = Mock({"getDescriptor" : 1515})
        conn = Mock({'getConnector': connector})
        self.assertEqual(len(em.reader_set), 0)
        em.addReader(conn)
        self.assertEqual(len(em.reader_set), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 1)
        em.addReader(conn) # do not add if already present
        self.assertEqual(len(em.reader_set), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 1)
        em.removeReader(conn)
        self.assertEqual(len(em.reader_set), 0)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 2)
        em.removeReader(conn)
        self.assertEqual(len(em.reader_set), 0)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 2)

        # add/removeWriter
        connector = Mock({"getDescriptor" : 1515})
        conn = Mock({'getConnector': connector})
        self.assertEqual(len(em.writer_set), 0)
        em.addWriter(conn)
        self.assertEqual(len(em.writer_set), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 3)
        em.addWriter(conn) # do not add if already present
        self.assertEqual(len(em.writer_set), 1)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 3)
        em.removeWriter(conn)
        self.assertEqual(len(em.writer_set), 0)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 4)
        em.removeWriter(conn)
        self.assertEqual(len(em.writer_set), 0)
        self.assertEquals(len(em.epoll.mockGetNamedCalls("modify")), 4)

        # poll
        r_connector = Mock({"getDescriptor" : 14515})
        r_conn = Mock({'getConnector': r_connector})
        em.register(r_conn)
        w_connector = Mock({"getDescriptor" : 351621})
        w_conn = Mock({'getConnector': w_connector})
        em.register(w_conn)
        em.epoll = Mock({"poll":(
          (r_connector.getDescriptor(),),
          (w_connector.getDescriptor(),),
        )})
        em.poll(timeout=10)
        # check it called poll on epoll
        self.assertEquals(len(em.epoll.mockGetNamedCalls("poll")), 1)
        call = em.epoll.mockGetNamedCalls("poll")[0]
        data = call.getParam(0)
        self.assertEqual(data, 10)
        # need to rebuild completely this test and the the packet queue
        # check readable conn
        #self.assertEquals(len(r_conn.mockGetNamedCalls("lock")), 1)
        #self.assertEquals(len(r_conn.mockGetNamedCalls("unlock")), 1)
        #self.assertEquals(len(r_conn.mockGetNamedCalls("readable")), 1)
        #self.assertEquals(len(r_conn.mockGetNamedCalls("writable")), 0)
        # check writable conn
        #self.assertEquals(len(w_conn.mockGetNamedCalls("lock")), 1)
        #self.assertEquals(len(w_conn.mockGetNamedCalls("unlock")), 1)
        #self.assertEquals(len(w_conn.mockGetNamedCalls("readable")), 0)
        #self.assertEquals(len(w_conn.mockGetNamedCalls("writable")), 1)

    def test_02_IdleEvent(self):
        # test init
        handler = Mock()
        conn = Mock({"getAddress" : ("127.9.9.9", 135),
                     "getHandler" : handler})
        event = IdleEvent(conn, 1, 10, 20)
        self.assertEqual(event.getId(), 1)
        self.assertNotEqual(event.getTime(), None)
        time = event.getTime()
        self.assertNotEqual(event.getCriticalTime(), None)
        critical_time = event.getCriticalTime()
        self.assertEqual(critical_time, time+20)

        # call with t < time < critical_time
        t = time - 10
        r = event(t)
        self.assertFalse(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 0)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 0)

        # call with time < t < critical_time
        t = time + 5
        self.assertTrue(t < critical_time)
        r = event(t)
        self.assertFalse(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("ping")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 0)

        # call with time < critical_time < t
        t = critical_time + 5
        self.assertTrue(t > critical_time)
        r = event(t)
        self.assertTrue(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 2)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 2)
        self.assertEquals(len(conn.mockGetNamedCalls("ping")), 1)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 1)

        # same test with additional time < 5
        # test init
        handler = Mock()
        conn = Mock({"getAddress" : ("127.9.9.9", 135),
                     "getHandler" : handler})
        event = IdleEvent(conn, 1, 10, 3)
        self.assertEqual(event.getId(), 1)
        self.assertNotEqual(event.getTime(), None)
        time = event.getTime()
        self.assertNotEqual(event.getCriticalTime(), None)
        critical_time = event.getCriticalTime()
        self.assertEqual(critical_time, time+3)

        # call with t < time < critical_time
        t = time - 10
        r = event(t)
        self.assertFalse(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 0)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 0)

        # call with time < t < critical_time
        t = time + 1
        self.assertTrue(t < critical_time)
        r = event(t)
        self.assertFalse(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 0)

        # call with time < critical_time < t
        t = critical_time + 5
        self.assertTrue(t > critical_time)
        r = event(t)
        self.assertTrue(r)
        self.assertEquals(len(conn.mockGetNamedCalls("lock")), 2)
        self.assertEquals(len(conn.mockGetNamedCalls("getHandler")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("close")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("unlock")), 2)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(handler.mockGetNamedCalls("timeoutExpired")), 1)


if __name__ == '__main__':
    unittest.main()
