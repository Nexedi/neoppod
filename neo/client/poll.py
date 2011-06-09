#
# Copyright (C) 2006-2010  Nexedi SA
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

from logging import DEBUG, ERROR
from threading import Thread, Event, enumerate as thread_enum
from neo.lib.locking import Lock
import neo.lib

class _ThreadedPoll(Thread):
    """Polling thread."""

    def __init__(self, em, **kw):
        Thread.__init__(self, **kw)
        self.em = em
        self.setDaemon(True)
        self._stop = Event()

    def run(self):
        _log = neo.lib.logging.log
        def log(*args, **kw):
            # Ignore errors due to garbage collection on exit
            try:
                _log(*args, **kw)
            except:
                if not self.stopping():
                    raise
        log(DEBUG, 'Started %s', self)
        while not self.stopping():
            try:
                # XXX: Delay cannot be infinite here, because we need
                #      to check connection timeout and thread shutdown.
                self.em.poll(1)
            except:
                log(ERROR, 'poll raised, retrying', exc_info=1)
        log(DEBUG, 'Threaded poll stopped')
        self._stop.clear()

    def stop(self):
        self._stop.set()

    def stopping(self):
        return self._stop.isSet()

class ThreadedPoll(object):
    """
    Wrapper for polloing thread, just to be able to start it again when
    it stopped.
    """
    _thread = None
    _started = False

    def __init__(self, *args, **kw):
        lock = Lock()
        self._status_lock_acquire = lock.acquire
        self._status_lock_release = lock.release
        self._args = args
        self._kw = kw
        self.newThread()

    def newThread(self):
        self._thread = _ThreadedPoll(*self._args, **self._kw)

    def start(self):
        """
        Start thread if not started or restart it if it's shutting down.
        """
        # TODO: a refcount-based approach would be better, but more intrusive.
        self._status_lock_acquire()
        try:
            thread = self._thread
            if thread.stopping():
                # XXX: ideally, we should wake thread up here, to be sure not
                # to wait forever.
                thread.join()
            if not thread.isAlive():
                if self._started:
                    self.newThread()
                else:
                    self._started = True
                self._thread.start()
        finally:
            self._status_lock_release()

    def stop(self):
        self._status_lock_acquire()
        try:
            self._thread.stop()
        finally:
            self._status_lock_release()

    def __getattr__(self, key):
        return getattr(self._thread, key)

    def __repr__(self):
        return repr(self._thread)

def psThreadedPoll(log=None):
    """
    Logs alive ThreadedPoll threads.
    """
    if log is None:
        log = neo.lib.logging.debug
    for thread in thread_enum():
        if not isinstance(thread, ThreadedPoll):
            continue
        log('Thread %s at 0x%x, %s', thread.getName(), id(thread),
            thread._stop.isSet() and 'stopping' or 'running')

