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

class ThreadedPoll(Thread):
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
        self.start()

    def run(self):
        while not self._stop.isSet():
            # First check if we receive any new message from other node
            try:
                self.em.poll()
            except:
                self.neo.logging.error('poll raised, retrying', exc_info=1)
        self.neo.logging.debug('Threaded poll stopped')

    def stop(self):
        self._stop.set()

def psThreadedPoll(log=None):
    """
    Logs alive ThreadedPoll threads.
    """
    if log is None:
        log = neo.logging.info
    for thread in thread_enum():
        if not isinstance(thread, ThreadedPoll):
            continue
        log('Thread %s at 0x%x, %s', thread.getName(), id(thread),
            thread._stop.isSet() and 'stopping' or 'running')

