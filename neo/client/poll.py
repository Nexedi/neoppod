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

from threading import Thread, Event, enumerate as thread_enum
import neo

class _ThreadedPoll(Thread):
    """Polling thread."""

    # Garbage collector hint:
    # Prevent logging module from being garbage-collected as it is needed for
    # run method to cleanly exit.
    neo = neo

    def __init__(self, em, **kw):
        Thread.__init__(self, **kw)
        self.em = em
        self.setDaemon(True)
        self._stop = Event()

    def run(self):
        neo.logging.debug('Started %s', self)
        while not self.stopping():
            # First check if we receive any new message from other node
            try:
                # XXX: Delay cannot be infinite here, unless we have a way to
                # interrupt this call when stopping.
                self.em.poll(1)
            except:
                self.neo.logging.error('poll raised, retrying', exc_info=1)
        self.neo.logging.debug('Threaded poll stopped')
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
        self._args = args
        self._kw = kw
        self.newThread()

    def newThread(self):
        self._thread = _ThreadedPoll(*self._args, **self._kw)

    def start(self):
        if self._started:
            self.newThread()
        else:
            self._started = True
        self._thread.start()

    def __getattr__(self, key):
        return getattr(self._thread, key)

    def __repr__(self):
        return repr(self._thread)

def psThreadedPoll(log=None):
    """
    Logs alive ThreadedPoll threads.
    """
    if log is None:
        log = neo.logging.debug
    for thread in thread_enum():
        if not isinstance(thread, ThreadedPoll):
            continue
        log('Thread %s at 0x%x, %s', thread.getName(), id(thread),
            thread._stop.isSet() and 'stopping' or 'running')

