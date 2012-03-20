#
# Copyright (C) 2006-2012  Nexedi SA
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

from time import time
from select import epoll, EPOLLIN, EPOLLOUT, EPOLLERR, EPOLLHUP
from errno import EINTR, EAGAIN
from . import logging
from .profiling import profiler_decorator

class EpollEventManager(object):
    """This class manages connections and events based on epoll(5)."""

    def __init__(self):
        self.connection_dict = {}
        self.reader_set = set()
        self.writer_set = set()
        self.epoll = epoll()
        self._pending_processing = []

    def close(self):
        for c in self.connection_dict.values():
            c.close()
        del self.__dict__

    def getConnectionList(self):
        # XXX: use index
        return [x for x in self.connection_dict.itervalues()
            if not x.isAborted()]

    def getClientList(self):
        # XXX: use index
        return [c for c in self.getConnectionList() if c.isClient()]

    def getServerList(self):
        # XXX: use index
        return [c for c in self.getConnectionList() if c.isServer()]

    def getConnectionListByUUID(self, uuid):
        """ Return the connection associated to the UUID, None if the UUID is
        None, invalid or not found"""
        # XXX: use index
        # XXX: consider remove UUID from connection and thus this method
        if uuid is None:
            return None
        result = []
        append = result.append
        for conn in self.getConnectionList():
            if conn.getUUID() == uuid:
                append(conn)
        return result

    def register(self, conn):
        fd = conn.getConnector().getDescriptor()
        self.connection_dict[fd] = conn
        self.epoll.register(fd)

    def unregister(self, conn):
        new_pending_processing = [x for x in self._pending_processing
                                  if x is not conn]
        # Check that we removed at most one entry from
        # self._pending_processing .
        assert len(new_pending_processing) > len(self._pending_processing) - 2
        self._pending_processing = new_pending_processing
        fd = conn.getConnector().getDescriptor()
        self.epoll.unregister(fd)
        del self.connection_dict[fd]

    def isIdle(self):
        return not (self._pending_processing or self.writer_set)

    def _addPendingConnection(self, conn):
        pending_processing = self._pending_processing
        if conn not in pending_processing:
            pending_processing.append(conn)

    def poll(self, timeout=1):
        if not self._pending_processing:
            # Fetch messages from polled file descriptors
            self._poll(timeout=timeout)
            if not self._pending_processing:
                return
        to_process = self._pending_processing.pop(0)
        to_process.lock()
        try:
            try:
                to_process.process()
            finally:
                # ...and requeue if there are pending messages
                if to_process.hasPendingMessages():
                    self._addPendingConnection(to_process)
        finally:
            to_process.unlock()
        # Non-blocking call: as we handled a packet, we should just offer
        # poll a chance to fetch & send already-available data, but it must
        # not delay us.
        self._poll(timeout=0)

    def _poll(self, timeout=1):
        try:
            event_list = self.epoll.poll(timeout)
        except IOError, exc:
            if exc.errno in (0, EAGAIN):
                logging.info('epoll.poll triggered undocumented error %r',
                    exc.errno)
            elif exc.errno != EINTR:
                raise
            event_list = ()
        wlist = []
        elist = []
        for fd, event in event_list:
            if event & EPOLLIN:
                conn = self.connection_dict[fd]
                conn.lock()
                try:
                    conn.readable()
                finally:
                    conn.unlock()
                if conn.hasPendingMessages():
                    self._addPendingConnection(conn)
            if event & EPOLLOUT:
                wlist.append(fd)
            if event & (EPOLLERR | EPOLLHUP):
                elist.append(fd)

        for fd in wlist:
            # This can fail, if a connection is closed in readable().
            try:
                conn = self.connection_dict[fd]
            except KeyError:
                continue
            conn.lock()
            try:
                conn.writable()
            finally:
                conn.unlock()

        for fd in elist:
            # This can fail, if a connection is closed in previous calls to
            # readable() or writable().
            try:
                conn = self.connection_dict[fd]
            except KeyError:
                continue
            conn.lock()
            try:
                conn.readable()
            finally:
                conn.unlock()
            if conn.hasPendingMessages():
                self._addPendingConnection(conn)

        t = time()
        for conn in self.connection_dict.values():
            conn.lock()
            try:
                conn.checkTimeout(t)
            finally:
                conn.unlock()

    def addReader(self, conn):
        connector = conn.getConnector()
        assert connector is not None, conn.whoSetConnector()
        fd = connector.getDescriptor()
        if fd not in self.reader_set:
            self.reader_set.add(fd)
            self.epoll.modify(fd, EPOLLIN | (
                fd in self.writer_set and EPOLLOUT))

    def removeReader(self, conn):
        connector = conn.getConnector()
        assert connector is not None, conn.whoSetConnector()
        fd = connector.getDescriptor()
        if fd in self.reader_set:
            self.reader_set.remove(fd)
            self.epoll.modify(fd, fd in self.writer_set and EPOLLOUT)

    @profiler_decorator
    def addWriter(self, conn):
        connector = conn.getConnector()
        assert connector is not None, conn.whoSetConnector()
        fd = connector.getDescriptor()
        if fd not in self.writer_set:
            self.writer_set.add(fd)
            self.epoll.modify(fd, EPOLLOUT | (
                fd in self.reader_set and EPOLLIN))

    def removeWriter(self, conn):
        connector = conn.getConnector()
        assert connector is not None, conn.whoSetConnector()
        fd = connector.getDescriptor()
        if fd in self.writer_set:
            self.writer_set.remove(fd)
            self.epoll.modify(fd, fd in self.reader_set and EPOLLIN)

    def log(self):
        logging.info('Event Manager:')
        logging.info('  Readers: %r', list(self.reader_set))
        logging.info('  Writers: %r', list(self.writer_set))
        logging.info('  Connections:')
        pending_set = set(self._pending_processing)
        for fd, conn in self.connection_dict.items():
            logging.info('    %r: %r (pending=%r)', fd, conn,
                conn in pending_set)


# Default to EpollEventManager.
EventManager = EpollEventManager
