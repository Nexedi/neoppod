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

from neo import protocol
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
            # No answer after _critical_time, close connection.
            # This means that remote peer is processing the request for too
            # long, although being responsive at network level.
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
            # Still no answer after _time, send a ping to see if connection is
            # broken.
            # Sending a ping triggers a new IdleEvent for the ping (hard timeout
            # after 5 seconds, see part on additional_timeout above).
            # XXX: Here, we return True, which causes the current IdleEvent
            # instance to be discarded, and a new instance is created with
            # reduced additional_timeout. It must be possible to avoid
            # recreating a new instance just to keep waiting for the same
            # response.
            # XXX: This code has no meaning if the remote peer is single-
            # threaded. Nevertheless, it should be kept in case it gets
            # multithreaded, someday (master & storage are the only candidates
            # for using this code, as other don't receive requests).
            conn.lock()
            try:
                if self._additional_timeout > 5:
                    # XXX this line is misleading: we modify self, but this
                    # instance is doomed anyway: we will return True, causing
                    # it to be discarded.
                    self._additional_timeout -= 5
                    conn.expectMessage(self._id, 5, self._additional_timeout)
                    # Start a keep-alive packet.
                    conn.ask(protocol.ping(), 5, 0)
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
        self._pending_processing = []

    def getConnectionList(self, with_admin_nodes=False):
        return self.connection_dict.values()

    def register(self, conn):
        self.connection_dict[conn.getConnector()] = conn

    def unregister(self, conn):
        del self.connection_dict[conn.getConnector()]

    def _getPendingConnection(self):
        if len(self._pending_processing):
            result = self._pending_processing.pop(0)
        else:
            result = None
        return result

    def _addPendingConnection(self, conn):
        self._pending_processing.append(conn)

    def poll(self, timeout = 1):
        to_process = self._getPendingConnection()
        if to_process is None:
            # Fetch messages from polled file descriptors
            self._poll(timeout=timeout)
            # See if there is anything to process
            to_process = self._getPendingConnection()
        if to_process is not None:
            # Process
            to_process.process()
            # ...and requeue if there are pending messages
            if to_process.hasPendingMessages():
                self._addPendingConnection(to_process)

    def _poll(self, timeout = 1):
        rlist, wlist, xlist = select(self.reader_set, self.writer_set, self.exc_list,
                                     timeout)
        for s in rlist:
            conn = self.connection_dict[s]
            conn.lock()
            try:
                conn.readable()
            finally:
                conn.unlock()
            if conn.hasPendingMessages():
                self._addPendingConnection(conn)

        for s in wlist:
            # This can fail, if a connection is closed in readable().
            try:
                conn = self.connection_dict[s]
            except KeyError:
                pass
            else:
                conn.lock()
                try:
                    conn.writable()
                finally:
                    conn.unlock()

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
        self._pending_processing = []

    def getConnectionList(self):
        return self.connection_dict.values()

    def getConnectionByUUID(self, uuid):
        """ Return the connection associated to the UUID, None if the UUID is
        None, invalid or not found"""
        # FIXME: We may optimize this by using use a dict on UUIDs
        if uuid is None:
            return None
        for conn in self.connection_dict.values():
            if conn.getUUID() == uuid:
                return conn
        return None

    def register(self, conn):
        fd = conn.getConnector().getDescriptor()
        self.connection_dict[fd] = conn
        self.epoll.register(fd)

    def unregister(self, conn):
        fd = conn.getConnector().getDescriptor()
        self.epoll.unregister(fd)
        del self.connection_dict[fd]

    def _getPendingConnection(self):
        if len(self._pending_processing):
            result = self._pending_processing.pop(0)
        else:
            result = None
        return result

    def _addPendingConnection(self, conn):
        self._pending_processing.append(conn)

    def poll(self, timeout = 1):
        to_process = self._getPendingConnection()
        if to_process is None:
            # Fetch messages from polled file descriptors
            self._poll(timeout=timeout)
            # See if there is anything to process
            to_process = self._getPendingConnection()
        if to_process is not None:
            # Process
            to_process.process()
            # ...and requeue if there are pending messages
            if to_process.hasPendingMessages():
                self._addPendingConnection(to_process)

    def _poll(self, timeout = 1):
        rlist, wlist = self.epoll.poll(timeout)
        for fd in rlist:
            try:
                conn = self.connection_dict[fd]
            except KeyError:
                pass
            else:
                conn.lock()
                try:
                    conn.readable()
                finally:
                    conn.unlock()
                if conn.hasPendingMessages():
                    self._addPendingConnection(conn)

        for fd in wlist:
            # This can fail, if a connection is closed in readable().
            try:
                conn = self.connection_dict[fd]
            except KeyError:
                pass
            else:
                conn.lock()
                try:
                    conn.writable()
                finally:
                    conn.unlock()

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
            fd = conn.getConnector().getDescriptor()
            if fd not in self.reader_set:
                self.reader_set.add(fd)
                self.epoll.modify(fd, 1, fd in self.writer_set)
        except AttributeError:
            pass

    def removeReader(self, conn):
        try:
            fd = conn.getConnector().getDescriptor()
            if fd in self.reader_set:
                self.reader_set.remove(fd)
                self.epoll.modify(fd, 0, fd in self.writer_set)
        except AttributeError:
            pass

    def addWriter(self, conn):
        try:
            fd = conn.getConnector().getDescriptor()
            if fd not in self.writer_set:
                self.writer_set.add(fd)
                self.epoll.modify(fd, fd in self.reader_set, 1)
        except AttributeError:
            pass

    def removeWriter(self, conn):
        try:
            fd = conn.getConnector().getDescriptor()
            if fd in self.writer_set:
                self.writer_set.remove(fd)
                self.epoll.modify(fd, fd in self.reader_set, 0)
        except AttributeError:
            pass

# Default to EpollEventManager.
EventManager = EpollEventManager
