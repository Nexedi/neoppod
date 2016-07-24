#
# Copyright (C) 2006-2016  Nexedi SA
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

import os, thread
from time import time
from select import epoll, EPOLLIN, EPOLLOUT, EPOLLERR, EPOLLHUP
from errno import EAGAIN, EEXIST, EINTR, ENOENT
from . import logging
from .locking import Lock

@apply
def dictionary_changed_size_during_iteration():
    d = {}; i = iter(d); d[0] = 0
    try:
        next(i)
    except RuntimeError as e:
        return str(e)
    raise AssertionError


class EpollEventManager(object):
    """This class manages connections and events based on epoll(5)."""

    _timeout = None
    _trigger_exit = False

    def __init__(self):
        self.connection_dict = {}
        self.reader_set = set()
        self.writer_set = set()
        self.epoll = epoll()
        self._pending_processing = []
        self._trigger_fd, w = os.pipe()
        os.close(w)
        self._trigger_lock = Lock()
        close_list = []
        self._closeAppend = close_list.append
        l = Lock()
        self._closeAcquire = l.acquire
        _release = l.release
        def release():
            try:
                while close_list:
                    close_list.pop()()
            finally:
                _release()
        self._closeRelease = release

    def close(self):
        os.close(self._trigger_fd)
        for c in self.connection_dict.values():
            c.close()
        del self.__dict__

    def getConnectionList(self):
        # XXX: use index
        while 1:
            # See _poll() about the use of self.connection_dict.itervalues()
            try:
                return [x for x in self.connection_dict.itervalues()
                          if not x.isAborted()]
            except RuntimeError, e:
                if str(e) != dictionary_changed_size_during_iteration:
                    raise
                logging.info("%r", e)

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

    # epoll_wait always waits for EPOLLERR & EPOLLHUP so we're forced
    # to unregister when we want to ignore all events for a connection.

    def register(self, conn, timeout_only=False):
        fd = conn.getConnector().getDescriptor()
        self.connection_dict[fd] = conn
        if timeout_only:
            self.wakeup()
        else:
            self.epoll.register(fd)
            self.addReader(conn)

    def unregister(self, conn, close=False):
        new_pending_processing = [x for x in self._pending_processing
                                  if x is not conn]
        # Check that we removed at most one entry from
        # self._pending_processing .
        assert len(new_pending_processing) > len(self._pending_processing) - 2
        self._pending_processing = new_pending_processing
        connector = conn.getConnector()
        fd = connector.getDescriptor()
        try:
            del self.connection_dict[fd]
            self.epoll.unregister(fd)
        except KeyError:
            pass
        except IOError, e:
            if e.errno != ENOENT:
                raise
        else:
            self.reader_set.discard(fd)
            self.writer_set.discard(fd)
        if close:
            self._closeAppend(connector.shutdown())
            if self._closeAcquire(0):
                self._closeRelease()

    def isIdle(self):
        return not (self._pending_processing or self.writer_set)

    def _addPendingConnection(self, conn):
        pending_processing = self._pending_processing
        if conn not in pending_processing:
            pending_processing.append(conn)

    def poll(self, blocking=1):
        if not self._pending_processing:
            # Fetch messages from polled file descriptors
            self._poll(blocking)
            if not self._pending_processing:
                return
        to_process = self._pending_processing.pop(0)
        try:
            to_process.process()
        finally:
            # ...and requeue if there are pending messages
            if to_process.hasPendingMessages():
                self._addPendingConnection(to_process)
        # Non-blocking call: as we handled a packet, we should just offer
        # poll a chance to fetch & send already-available data, but it must
        # not delay us.
        self._poll(0)

    def _poll(self, blocking):
        if blocking:
            # self.connection_dict may be changed at any time by another thread,
            # which may cause itervalues() to fail. But this happens so rarely,
            # that for performance reasons, we prefer to retry, rather than:
            # - protect self.connection_dict with a lock
            # - or iterate over an atomic copy.
            while 1:
                try:
                    timeout = self._timeout
                    timeout_object = self
                    for conn in self.connection_dict.itervalues():
                        t = conn.getTimeout()
                        if t and (timeout is None or t < timeout):
                            timeout = t
                            timeout_object = conn
                    break
                except RuntimeError, e:
                    if str(e) != dictionary_changed_size_during_iteration:
                        raise
                    logging.info("%r", e)
            # Make sure epoll_wait does not return too early, because it has a
            # granularity of 1ms and Python 2.7 rounds the timeout towards zero.
            # See also https://bugs.python.org/issue20452 (fixed in Python 3).
            blocking = .001 + max(0, timeout - time()) if timeout else -1
        # From this point, and until we have processed all fds returned by
        # epoll, we must prevent any fd from being closed, because they could
        # be reallocated by new connection, either by this thread or by another.
        # Sockets to close are queued, and they're really closed in the
        # 'finally' clause.
        self._closeAcquire()
        try:
            event_list = self.epoll.poll(blocking)
        except IOError, exc:
            if exc.errno in (0, EAGAIN):
                logging.info('epoll.poll triggered undocumented error %r',
                    exc.errno)
            elif exc.errno != EINTR:
                raise
            return
        else:
            if event_list:
                wlist = []
                elist = []
                for fd, event in event_list:
                    if event & EPOLLIN:
                        try:
                            conn = self.connection_dict[fd]
                        except KeyError:
                            continue
                        if conn.readable():
                            self._addPendingConnection(conn)
                    if event & EPOLLOUT:
                        wlist.append(fd)
                    if event & (EPOLLERR | EPOLLHUP):
                        elist.append(fd)
                for fd in wlist:
                    try:
                        conn = self.connection_dict[fd]
                    except KeyError:
                        continue
                    conn.writable()
                for fd in elist:
                    try:
                        conn = self.connection_dict[fd]
                    except KeyError:
                        if fd == self._trigger_fd:
                            with self._trigger_lock:
                                self.epoll.unregister(fd)
                                if self._trigger_exit:
                                    del self._trigger_exit
                                    thread.exit()
                        continue
                    if conn.readable():
                        self._addPendingConnection(conn)
                return
        finally:
            self._closeRelease()
        if blocking > 0:
            logging.debug('timeout triggered for %r', timeout_object)
            timeout_object.onTimeout()

    def onTimeout(self):
        on_timeout = self._on_timeout
        del self._on_timeout
        self._timeout = None
        on_timeout()

    def setTimeout(self, *args):
        self._timeout, self._on_timeout = args

    def wakeup(self, exit=False):
        with self._trigger_lock:
            self._trigger_exit |= exit
            try:
                self.epoll.register(self._trigger_fd)
            except IOError, e:
                # Ignore if 'wakeup' is called several times in a row.
                if e.errno != EEXIST:
                    raise

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
