#
# Copyright (C) 2006-2019  Nexedi SA
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

from six.moves import _thread
import threading, weakref
from . import debug, logging
from .app import BaseApplication
from .dispatcher import Dispatcher
from .locking import SimpleQueue

class app_set(weakref.WeakSet):

    def on_log(self):
        for app in self:
            app.log()

app_set = app_set()

def registerLiveDebugger():
    debug.register(app_set.on_log)
registerLiveDebugger()


class ThreadContainer(threading.local):

    def __init__(self):
        self.queue = SimpleQueue()
        self.answer = None


class ThreadedApplication(BaseApplication):
    """The client node application."""

    uuid = None

    def __init__(self, master_nodes, name, **kw):
        super(ThreadedApplication, self).__init__(**kw)
        self.poll_thread = threading.Thread(target=self.run, name=name)
        self.poll_thread.daemon = True
        # Internal Attributes common to all thread
        self.name = name
        self.dispatcher = Dispatcher()
        self.master_conn = None
        self.nm.createMasters(master_nodes)

        # Internal attribute distinct between thread
        self._thread_container = ThreadContainer()
        app_set.add(self) # to register self.on_log

    def close(self):
        # Clear all connection
        self.master_conn = None
        if self.poll_thread.is_alive():
            for conn in self.em.getConnectionList():
                conn.close()
            # Stop polling thread
            logging.debug('Stopping %s', self.poll_thread)
            self.em.wakeup(_thread.exit)
        else:
            super(ThreadedApplication, self).close()

    def start(self):
        self.poll_thread.is_alive() or self.poll_thread.start()

    def run(self):
        logging.debug("Started %s", self.poll_thread)
        try:
            with self.em.wakeup_fd():
                self._run()
        finally:
            super(ThreadedApplication, self).close()
            logging.debug("Poll thread stopped")

    def _run(self):
        poll = self.em.poll
        while 1:
            try:
                while 1:
                    poll(1)
            except Exception:
                self.log()
                logging.error("poll raised, retrying", exc_info=1)

    def getHandlerData(self):
        return self._thread_container.answer

    def setHandlerData(self, data):
        self._thread_container.answer = data

    def _handlePacket(self, conn, packet, kw={}, handler=None):
        """
          conn
            The connection which received the packet (forwarded to handler).
          packet
            The packet to handle.
          handler
            The handler to use to handle packet.
            If not given, it will be guessed from connection's not type.
        """
        if handler is None:
            # Guess the handler to use based on the type of node on the
            # connection
            node = self.nm.getByAddress(conn.getAddress())
            if node is None:
                raise ValueError(
                  'Expecting an answer from a node '
                  'which type is not known... Is this right ?'
                )
            if node.isStorage():
                handler = self.storage_handler
            elif node.isMaster():
                handler = self.primary_handler
            else:
                raise ValueError('Unknown node type: %r' % (node.__class__, ))
        with conn.lock:
            handler.dispatch(conn, packet, kw)

    def _ask(self, conn, packet, handler=None, **kw):
        self.setHandlerData(None)
        queue = self._thread_container.queue
        msg_id = conn.ask(packet, queue=queue, **kw)
        get = queue.get
        _handlePacket = self._handlePacket
        while True:
            qconn, qpacket, kw = get(True)
            if conn is qconn and msg_id == qpacket.getId():
                _handlePacket(qconn, qpacket, kw, handler)
                break
            _handlePacket(qconn, qpacket, kw)
        return self.getHandlerData()
