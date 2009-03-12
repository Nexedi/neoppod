#
# Copyright (C) 2006-2009  Nexedi SA
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

import logging
from select import select
from time import time

from neo.protocol import Packet
from neo.epoll import Epoll

class IdleEvent(object):
    """This class represents an event called when a connection is waiting for
    a message too long."""

    def __init__(self, conn, msg_id, timeout, additional_timeout):
        self._conn = conn
        self._id = msg_id
        t = time()
        self._time = t + timeout
        self._critical_time = t + timeout + additional_timeout
        self._additional_timeout = additional_timeout

    def getId(self):
        return self._id

    def getTime(self):
        return self._time

    def getCriticalTime(self):
        return self._critical_time

    def __call__(self, t):
        conn = self._conn
        if t > self._critical_time:
            conn.lock()
            try:
                logging.info('timeout for %r with %s:%d', 
                             self._id, *(conn.getAddress()))
                conn.getHandler().timeoutExpired(conn)
                conn.close()
                return True
            finally:
                conn.unlock()
        elif t > self._time:
            conn.lock()
            try:
                if self._additional_timeout > 5:
                    self._additional_timeout -= 5
                    conn.expectMessage(self._id, 5, self._additional_timeout)
                    # Start a keep-alive packet.
                    logging.info('sending a ping to %s:%d', 
                                 *(conn.getAddress()))
                    msg_id = conn.getNextId()
                    conn.addPacket(Packet().ping(msg_id))
                    conn.expectMessage(msg_id, 5, 0)
                else:
                    conn.expectMessage(self._id, self._additional_timeout, 0)
                return True
            finally:
                conn.unlock()
        return False

class SelectEventManager(object):
    """This class manages connections and events based on select(2)."""

    def __init__(self):
        self.connection_dict = {}
        self.reader_set = set([])
        self.writer_set = set([])
        self.exc_list = []
        self.event_list = []
        self.prev_time = time()

    def getConnectionList(self):
        return self.connection_dict.values()

    def register(self, conn):
        self.connection_dict[conn.getConnector()] = conn

    def unregister(self, conn):
        del self.connection_dict[conn.getConnector()]

    def poll(self, timeout = 1):
        rlist, wlist, xlist = select(self.reader_set, self.writer_set, self.exc_list,
                                     timeout)
        for s in rlist:
            conn = self.connection_dict[s]
            conn.lock()
            try:
                conn.readable()
            finally:
                conn.unlock()

        for s in wlist:
            # This can fail, if a connection is closed in readable().
            try:
                conn = self.connection_dict[s]
                conn.lock()
                try:
                    conn.writable()
                finally:
                    conn.unlock()
            except KeyError:
                pass

        # Check idle events. Do not check them out too often, because this
        # is somehow heavy.
        event_list = self.event_list
        if event_list:
            t = time()
            if t - self.prev_time >= 1:
                self.prev_time = t
                event_list.sort(key = lambda event: event.getTime(), 
                                reverse = True)
                while event_list:
                    event = event_list.pop()
                    if event(t):
                        try:
                            event_list.remove(event)
                        except ValueError:
                            pass
                    else:
                        break

    def addIdleEvent(self, event):
        self.event_list.append(event)

    def removeIdleEvent(self, event):
        try:
            self.event_list.remove(event)
        except ValueError:
            pass

    def addReader(self, conn):
        self.reader_set.add(conn.getConnector())

    def removeReader(self, conn):
        self.reader_set.discard(conn.getConnector())

    def addWriter(self, conn):
        self.writer_set.add(conn.getConnector())

    def removeWriter(self, conn):
        self.writer_set.discard(conn.getConnector())

class EpollEventManager(object):
    """This class manages connections and events based on epoll(5)."""

    def __init__(self):
        self.connection_dict = {}
        self.reader_set = set([])
        self.writer_set = set([])
        self.event_list = []
        self.prev_time = time()
        self.epoll = Epoll()

    def getConnectionList(self):
        return self.connection_dict.values()

    def register(self, conn):
        fd = conn.getDescriptor()
        self.connection_dict[fd] = conn
        self.epoll.register(fd)

    def unregister(self, conn):
        fd = conn.getDescriptor()
        self.epoll.unregister(fd)
        del self.connection_dict[fd]

    def poll(self, timeout = 1):
        rlist, wlist = self.epoll.poll(timeout)
        for fd in rlist:
            try:
                conn = self.connection_dict[fd]
                #logging.info("conn is %s" %(conn,))
                conn.lock()
                try:
                    conn.readable()
                finally:
                    conn.unlock()
            except KeyError:
                pass

        for fd in wlist:
            # This can fail, if a connection is closed in readable().
            try:
                conn = self.connection_dict[fd]
                conn.lock()
                try:
                    conn.writable()
                finally:
                    conn.unlock()
            except KeyError:
                pass

        # Check idle events. Do not check them out too often, because this
        # is somehow heavy.
        event_list = self.event_list
        if event_list:
            t = time()
            if t - self.prev_time >= 1:
                self.prev_time = t
                event_list.sort(key = lambda event: event.getTime())
                while event_list:
                    event = event_list[0]
                    if event(t):
                        try:
                            event_list.remove(event)
                        except ValueError:
                            pass
                    else:
                        break

    def addIdleEvent(self, event):
        self.event_list.append(event)

    def removeIdleEvent(self, event):
        try:
            self.event_list.remove(event)
        except ValueError:
            pass

    def addReader(self, conn):
        try:
            fd = conn.getDescriptor()
            if fd not in self.reader_set:
                self.reader_set.add(fd)
                self.epoll.modify(fd, 1, fd in self.writer_set)
        except AttributeError:
            pass

    def removeReader(self, conn):
        try:
            fd = conn.getDescriptor()
            if fd in self.reader_set:
                self.reader_set.remove(fd)
                self.epoll.modify(fd, 0, fd in self.writer_set)
        except AttributeError:
            pass

    def addWriter(self, conn):
        try:
            fd = conn.getDescriptor()
            if fd not in self.writer_set:
                self.writer_set.add(fd)
                self.epoll.modify(fd, fd in self.reader_set, 1)
        except AttributeError:
            pass

    def removeWriter(self, conn):
        try:
            fd = conn.getDescriptor()
            if fd in self.writer_set:
                self.writer_set.remove(fd)
                self.epoll.modify(fd, fd in self.reader_set, 0)
        except AttributeError:
            pass

# Default to EpollEventManager.
EventManager = EpollEventManager
